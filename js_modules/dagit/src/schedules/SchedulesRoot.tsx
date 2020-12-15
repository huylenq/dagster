import {useQuery} from '@apollo/client';
import {NonIdealState} from '@blueprintjs/core';
import {IconNames} from '@blueprintjs/icons';
import React from 'react';

import {Loading} from 'src/Loading';
import {PythonErrorInfo} from 'src/PythonErrorInfo';
import {useDocumentTitle} from 'src/hooks/useDocumentTitle';
import {UnloadableJobs} from 'src/jobs/UnloadableJobs';
import {SCHEDULES_ROOT_QUERY, SchedulerTimezoneNote} from 'src/schedules/ScheduleUtils';
import {SchedulerInfo} from 'src/schedules/SchedulerInfo';
import {SchedulesTable} from 'src/schedules/SchedulesTable';
import {SchedulesRootQuery} from 'src/schedules/types/SchedulesRootQuery';
import {JobType} from 'src/types/globalTypes';
import {Group} from 'src/ui/Group';
import {Page} from 'src/ui/Page';
import {repoAddressToSelector} from 'src/workspace/repoAddressToSelector';
import {RepoAddress} from 'src/workspace/types';

export const SchedulesRoot = ({repoAddress}: {repoAddress: RepoAddress}) => {
  useDocumentTitle('Schedules');
  const repositorySelector = repoAddressToSelector(repoAddress);

  const queryResult = useQuery<SchedulesRootQuery>(SCHEDULES_ROOT_QUERY, {
    variables: {
      repositorySelector: repositorySelector,
      jobType: JobType.SCHEDULE,
    },
    fetchPolicy: 'cache-and-network',
    pollInterval: 50 * 1000,
    partialRefetch: true,
  });

  return (
    <Page>
      <Loading queryResult={queryResult} allowStaleData={true}>
        {(result) => {
          const {repositoryOrError, scheduler, unloadableJobStatesOrError} = result;
          let schedulesSection = null;
          let unloadableSchedulesSection = null;

          if (repositoryOrError.__typename === 'PythonError') {
            schedulesSection = <PythonErrorInfo error={repositoryOrError} />;
          } else if (unloadableJobStatesOrError.__typename === 'PythonError') {
            schedulesSection = <PythonErrorInfo error={unloadableJobStatesOrError} />;
          } else if (repositoryOrError.__typename === 'RepositoryNotFoundError') {
            schedulesSection = (
              <NonIdealState
                icon={IconNames.ERROR}
                title="Repository not found"
                description="Could not load this repository."
              />
            );
          } else {
            const schedules = repositoryOrError.schedules;
            if (!schedules.length) {
              schedulesSection = (
                <NonIdealState
                  icon={IconNames.ERROR}
                  title="No Schedules Found"
                  description={
                    <p>
                      This repository does not have any schedules defined. Visit the{' '}
                      <a href="https://docs.dagster.io/overview/scheduling-partitions/schedules">
                        scheduler documentation
                      </a>{' '}
                      for more information about scheduling pipeline runs in Dagster. .
                    </p>
                  }
                />
              );
            } else {
              schedulesSection = schedules.length > 0 && (
                <Group direction="column" spacing={16}>
                  <SchedulerTimezoneNote schedulerOrError={scheduler} />
                  <SchedulesTable
                    schedules={repositoryOrError.schedules}
                    repoAddress={repoAddress}
                  />
                </Group>
              );
            }
            unloadableSchedulesSection = unloadableJobStatesOrError.results.length > 0 && (
              <UnloadableJobs
                jobStates={unloadableJobStatesOrError.results}
                jobType={JobType.SCHEDULE}
              />
            );
          }

          return (
            <Group direction="column" spacing={20}>
              <SchedulerInfo schedulerOrError={scheduler} />
              {schedulesSection}
              {unloadableSchedulesSection}
            </Group>
          );
        }}
      </Loading>
    </Page>
  );
};
