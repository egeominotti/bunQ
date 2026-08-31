import type * as Core from './core';
import type * as Cron from './cron';
import type * as Dashboard from './dashboard';
import type * as Dlq from './dlq';
import type * as Extended from './extended';
import type * as Limits from './limits';
import type * as Management from './management';
import type * as Monitoring from './monitoring';
import type * as Protocol from './protocol';
import type * as Query from './query';
import type * as Workers from './workers';

export type Command =
  | Core.PushCommand
  | Core.PushBatchCommand
  | Core.PushFlowCommand
  | Core.PullCommand
  | Core.PullBatchCommand
  | Core.AckCommand
  | Core.AckBatchCommand
  | Core.FailCommand
  | Query.GetJobCommand
  | Query.GetStateCommand
  | Query.GetResultCommand
  | Query.GetJobsCommand
  | Query.GetJobCountsCommand
  | Query.GetCountsPerPriorityCommand
  | Query.GetJobByCustomIdCommand
  | Query.CountCommand
  | Query.GetProgressCommand
  | Query.GetChildrenValuesCommand
  | Management.CancelCommand
  | Management.ProgressCommand
  | Management.UpdateCommand
  | Management.ChangePriorityCommand
  | Management.PromoteCommand
  | Management.WaitJobCommand
  | Management.MoveToDelayedCommand
  | Management.DiscardCommand
  | Management.PauseCommand
  | Management.ResumeCommand
  | Management.IsPausedCommand
  | Management.DrainCommand
  | Management.ObliterateCommand
  | Management.ListQueuesCommand
  | Management.CleanCommand
  | Dlq.DlqCommand
  | Dlq.GetDlqStatsCommand
  | Dlq.RetryDlqCommand
  | Dlq.PurgeDlqCommand
  | Dlq.RemoveDlqJobCommand
  | Dlq.RetryCompletedCommand
  | Dlq.SetDlqConfigCommand
  | Dlq.GetDlqConfigCommand
  | Limits.GetQueueLimitsCommand
  | Limits.GetDeduplicationJobIdCommand
  | Limits.RemoveDeduplicationKeyCommand
  | Limits.RemoveJobDeduplicationKeyCommand
  | Limits.MoveToWaitingChildrenCommand
  | Limits.RateLimitCommand
  | Limits.SetConcurrencyCommand
  | Limits.RateLimitClearCommand
  | Limits.ClearConcurrencyCommand
  | Limits.SetStallConfigCommand
  | Limits.GetStallConfigCommand
  | Limits.GetGroupJobsCountCommand
  | Limits.GetGroupsJobsCountCommand
  | Limits.GetGroupActiveCountCommand
  | Limits.SetGroupRateLimitCommand
  | Limits.GetGroupRateLimitCommand
  | Limits.RemoveGroupRateLimitCommand
  | Limits.GetGroupRateLimitTtlCommand
  | Limits.SetGroupConcurrencyCommand
  | Limits.GetGroupConcurrencyCommand
  | Limits.RemoveGroupConcurrencyCommand
  | Cron.CronCommand
  | Cron.CronDeleteCommand
  | Cron.CronListCommand
  | Cron.CronGetCommand
  | Monitoring.AddLogCommand
  | Monitoring.GetLogsCommand
  | Monitoring.AddWebhookCommand
  | Monitoring.RemoveWebhookCommand
  | Monitoring.ListWebhooksCommand
  | Monitoring.StatsCommand
  | Monitoring.MetricsCommand
  | Monitoring.TrimEventsCommand
  | Monitoring.PrometheusCommand
  | Monitoring.StorageStatusCommand
  | Workers.HeartbeatCommand
  | Workers.JobHeartbeatCommand
  | Workers.JobHeartbeatBatchCommand
  | Workers.PingCommand
  | Workers.RegisterWorkerCommand
  | Workers.UnregisterWorkerCommand
  | Workers.ListWorkersCommand
  | Extended.ClearLogsCommand
  | Extended.ExtendLockCommand
  | Extended.ExtendLocksCommand
  | Extended.ChangeDelayCommand
  | Extended.SetWebhookEnabledCommand
  | Extended.CompactMemoryCommand
  | Extended.UpdateParentCommand
  | Extended.GetFailedChildrenValuesCommand
  | Extended.GetIgnoredChildrenFailuresCommand
  | Extended.RemoveChildDependencyCommand
  | Extended.RemoveUnprocessedChildrenCommand
  | Extended.MoveToWaitCommand
  | Extended.PromoteJobsCommand
  | Dashboard.DashboardOverviewCommand
  | Dashboard.DashboardQueuesCommand
  | Dashboard.DashboardQueueCommand
  | Protocol.AuthCommand
  | Protocol.HelloCommand
  | Protocol.SubscribeEventsCommand
  | Protocol.UnsubscribeEventsCommand;

export type CommandType = Command['cmd'];
