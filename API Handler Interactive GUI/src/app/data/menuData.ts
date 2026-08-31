export interface Action {
  id: string;
  title: string;
  description: string;
  icon?: string;
  badge?: string;
}

export interface Category {
  id: string;
  title: string;
  description: string;
  icon: string;
  actions: Action[];
}

export interface MenuData {
  categories: Category[];
}

export const menuData: MenuData = {
  categories: [
    {
      id: '1',
      title: 'CompanyCam API',
      description: 'Webhook registration, local webhook tests, and missed-comment recovery',
      icon: 'Webhook',
      actions: [
        {
          id: '1',
          title: 'Webhook & Integration Tests',
          description: 'Send test payloads and verify CompanyCam webhook setup'
        },
        {
          id: '2',
          title: 'Missed Comment Catch-up',
          description: 'Run recovery flow for missed webhook comments'
        },
        {
          id: '3',
          title: 'Verify Webhook Registration',
          description: 'Check current CompanyCam webhook registration status'
        }
      ]
    },
    {
      id: '2',
      title: 'MarketSharp Tagging API',
      description: 'Queue operations, mention/tagging flows, and mapping controls',
      icon: 'Database',
      actions: [
        {
          id: '1',
          title: 'Queue Status',
          description: 'View queue totals by processing state'
        },
        {
          id: '2',
          title: 'Browse & Manage Queue',
          description: 'Inspect queue items and requeue/edit/delete'
        },
        {
          id: '3',
          title: 'Requeue All Unmatched',
          description: 'Bulk requeue unmatched items'
        },
        {
          id: '4',
          title: 'Review True-Fail Items',
          description: 'Requeue or rename permanently failed rows'
        },
        {
          id: '5',
          title: 'Re-push Posted Comments',
          description: 'Replay posted comments through queue worker'
        },
        {
          id: '6',
          title: 'Contact Mapping',
          description: 'Manage project/contact URL mapping overrides'
        },
        {
          id: '7',
          title: 'Duplicate Check',
          description: 'Scan queue DB for duplicate event IDs and texts'
        },
        {
          id: '8',
          title: 'Tagger Mention Recovery',
          description: 'Replay tagger @mention emails for missed MarketSharp notes'
        }
      ]
    },
    {
      id: '3',
      title: 'Google Click Ad Reporting',
      description: 'GCLID conversion exports and reporting audits',
      icon: 'BarChart3',
      actions: [
        {
          id: '1',
          title: 'Run report — this month',
          description: 'Build and export conversion report for current month'
        },
        {
          id: '2',
          title: 'Run report — last month',
          description: 'Build and export conversion report for previous month'
        },
        {
          id: '3',
          title: 'Run report — custom date range',
          description: 'Build report from a custom start date'
        },
        {
          id: '4',
          title: 'Preview report in terminal',
          description: 'Generate rows and print preview only'
        },
        {
          id: '5',
          title: 'Show last exported CSV',
          description: 'List most recent conversion export files'
        },
        {
          id: '6',
          title: 'Run eligibility audit + summary',
          description: 'Create audit and summary artifacts for a month'
        },
        {
          id: '7',
          title: 'Backend contact roster reconciliation',
          description: 'Compare exportable contacts vs backend source roster'
        },
        {
          id: '8',
          title: 'Export backend source roster',
          description: 'Export full backend matched-contact roster CSV'
        },
        {
          id: '9',
          title: 'CSV file manager',
          description: 'Inspect and manage generated CSV files'
        }
      ]
    },
    {
      id: '4',
      title: 'Database Administration',
      description: 'DB backup, integrity, consolidation, and audit access',
      icon: 'HardDrive',
      actions: [
        {
          id: '1',
          title: 'Show DB paths and row counts',
          description: 'Display active DB files and current row totals'
        },
        {
          id: '2',
          title: 'Backup active DB files now',
          description: 'Create timestamped backups of live DB files'
        },
        {
          id: '3',
          title: 'Run SQLite integrity checks',
          description: 'Run PRAGMA integrity_check on discovered DB files'
        },
        {
          id: '4',
          title: 'Consolidate queue DBs',
          description: 'Merge duplicate queue DBs into canonical path'
        },
        {
          id: '5',
          title: 'Audit Log',
          description: 'Inspect and export posted-comment audit history'
        },
        {
          id: '6',
          title: 'DB Status Summary',
          description: 'Print active DB paths, integrity, and queue counts'
        }
      ]
    },
    {
      id: '5',
      title: 'System Maintenance',
      description: 'Service diagnostics, health checks, and predeploy validation',
      icon: 'Settings',
      actions: [
        {
          id: '1',
          title: 'Restart queue workers',
          description: 'Restart queue and event worker services'
        },
        {
          id: '2',
          title: 'Restart all services',
          description: 'Restart workers plus API and true-fail checker'
        },
        {
          id: '3',
          title: 'View worker journal',
          description: 'Show recent systemd logs for queue workers'
        },
        {
          id: '4',
          title: 'Check local health endpoint',
          description: 'Call local /health and print status'
        },
        {
          id: '5',
          title: 'Show env config summary',
          description: 'Print key runtime environment settings'
        },
        {
          id: '6',
          title: 'MarketSharp mention worker check',
          description: 'Run packaged mention-worker health script'
        },
        {
          id: '7',
          title: 'Predeploy DB Checks',
          description: 'Run non-interactive DB preflight checks'
        }
      ]
    }
  ]
};
