from __future__ import annotations

import functools
from inspect import Parameter, Signature, isgeneratorfunction, signature
from typing import (
    Any,
    Callable,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    get_type_hints,
)

from dagster._seven import is_module_available

from .utils import NoValueSentinel

IS_DOCSTRING_PARSER_AVAILABLE = is_module_available("docstring_parser")


class InferredInputProps(NamedTuple):
    """The information about an input that can be inferred from the function signature."""

    name: str
    annotation: Any
    description: Optional[str]
    default_value: Any = NoValueSentinel


class InferredOutputProps(NamedTuple):
    """The information about an input that can be inferred from the function signature."""

    annotation: Any
    description: Optional[str]


def _infer_input_description_from_docstring(fn: Callable) -> Mapping[str, Optional[str]]:
    doc_str = fn.__doc__
    if not IS_DOCSTRING_PARSER_AVAILABLE or doc_str is None:
        return {}

    from docstring_parser import parse

    try:
        docstring = parse(doc_str)
        return {p.arg_name: p.description for p in docstring.params}
    except Exception:
        return {}


def _infer_output_description_from_docstring(fn: Callable) -> Optional[str]:
    doc_str = fn.__doc__
    if not IS_DOCSTRING_PARSER_AVAILABLE or doc_str is None:
        return None
    from docstring_parser import parse

    try:
        docstring = parse(doc_str)
        if docstring.returns is None:
            return None

        return docstring.returns.description
    except Exception:
        return None


def _get_type_hints(fn: Callable) -> Mapping[str, Any]:
    from dagster._core.execution.context.compute import (
        OpExecutionContext,
    )

    localns = {"OpExecutionContext": OpExecutionContext}

    # It is necessary to inject `OpExecutionContext` into the local namespace because there are some
    # situations where we dynamically generate an op compute function with signature:
    #
    # def fn(context: OpExecutionContext, ...):
    #
    # If `OpExecutionContext` is imported in function scope (i.e. not module-level scope) when this
    # function is constructed, then `OpExecutionContext` will not be available in the default scope
    # used by `typing.get_type_hints` when resolving string annotations / forward references.
    target = fn.func if isinstance(fn, functools.partial) else fn
    return get_type_hints(target, localns=localns)


def infer_output_props(fn: Callable) -> InferredOutputProps:
    type_hints = _get_type_hints(fn)
    annotation = (
        type_hints["return"]
        if not isgeneratorfunction(fn) and "return" in type_hints
        else Parameter.empty
    )

    return InferredOutputProps(
        annotation=annotation,
        description=_infer_output_description_from_docstring(fn),
    )


def has_explicit_return_type(fn: Callable) -> bool:
    sig = signature(fn)
    return sig.return_annotation is not Signature.empty


def _infer_inputs_from_params(
    params: Sequence[Parameter],
    type_hints: Mapping[str, object],
    descriptions: Optional[Mapping[str, Optional[str]]] = None,
) -> Sequence[InferredInputProps]:
    _descriptions: Mapping[str, Optional[str]] = descriptions or {}
    input_defs = []
    for param in params:
        if param.default is not Parameter.empty:
            input_def = InferredInputProps(
                param.name,
                type_hints.get(param.name, param.annotation),
                default_value=param.default,
                description=_descriptions.get(param.name),
            )
        else:
            input_def = InferredInputProps(
                param.name,
                type_hints.get(param.name, param.annotation),
                description=_descriptions.get(param.name),
            )

        input_defs.append(input_def)

    return input_defs


def infer_input_props(fn: Callable, context_arg_provided: bool) -> Sequence[InferredInputProps]:
    sig = signature(fn)
    params = list(sig.parameters.values())
    type_hints = _get_type_hints(fn)
    descriptions = _infer_input_description_from_docstring(fn)
    params_to_infer = params[1:] if context_arg_provided else params
    defs = _infer_inputs_from_params(params_to_infer, type_hints, descriptions=descriptions)
    return defs
