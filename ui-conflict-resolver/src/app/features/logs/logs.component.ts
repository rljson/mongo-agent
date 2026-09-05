import { CommonModule } from '@angular/common';
import { Component, ElementRef, OnDestroy, OnInit, ViewChild } from '@angular/core';
import { FormsModule } from '@angular/forms';

import { interval, Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';


type LogLevel = 'info' | 'warn' | 'error' | 'debug';

interface LogEntry {
  timestamp: Date;
  level: LogLevel;
  message: string;
  source: string;
}

@Component({
  selector: 'app-logs',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './logs.component.html',
  styleUrls: ['./logs.component.scss'],
})
export class LogsComponent implements OnInit, OnDestroy {
  @ViewChild('logContainer') logContainer?: ElementRef;

  logs: LogEntry[] = [];
  filteredLogs: LogEntry[] = [];

  // Filters
  selectedLevels: Set<LogLevel> = new Set(['info', 'warn', 'error', 'debug']);
  searchTerm: string = '';
  autoScroll: boolean = true;

  // Stats
  logCounts = {
    info: 0,
    warn: 0,
    error: 0,
    debug: 0,
  };

  private destroy$ = new Subject<void>();
  private maxLogs = 500; // Keep only last 500 logs

  // Simulated log sources and messages
  private logSources = [
    'sync-agent',
    'network',
    'conflict-resolver',
    'hub',
    'storage',
  ];
  private logMessages = {
    info: [
      'Sync operation completed successfully',
      'Connected to peer node',
      'Hash chain verified',
      'Document synchronized',
      'Health check passed',
      'Configuration loaded',
      'WebSocket connection established',
    ],
    warn: [
      'Retry attempt {n} for failed operation',
      'Network latency detected: {n}ms',
      'Approaching storage limit',
      'Peer response timeout',
      'Using fallback configuration',
    ],
    error: [
      'Failed to connect to MongoDB',
      'Conflict resolution failed',
      'Invalid hash chain detected',
      'Network partition detected',
      'Authentication failed',
    ],
    debug: [
      'Processing message: {data}',
      'Cache miss for key {key}',
      'Heartbeat sent to peer',
      'State transition: {old} -> {new}',
      'Memory usage: {n}MB',
    ],
  };

  ngOnInit(): void {
    // Add initial logs
    this.addInitialLogs();

    // Simulate real-time log streaming
    interval(2000)
      .pipe(takeUntil(this.destroy$))
      .subscribe(() => {
        this.generateRandomLog();
      });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  private addInitialLogs(): void {
    const initialMessages = [
      {
        level: 'info' as LogLevel,
        message: 'System startup initiated',
        source: 'hub',
      },
      {
        level: 'info' as LogLevel,
        message: 'MongoDB connection established',
        source: 'storage',
      },
      {
        level: 'info' as LogLevel,
        message: 'Sync agents initialized',
        source: 'sync-agent',
      },
      {
        level: 'debug' as LogLevel,
        message: 'Loaded configuration from env',
        source: 'hub',
      },
      {
        level: 'info' as LogLevel,
        message: 'Network topology formed',
        source: 'network',
      },
    ];

    initialMessages.forEach((msg) => {
      this.addLog(msg.level, msg.message, msg.source);
    });
  }

  private generateRandomLog(): void {
    const levels: LogLevel[] = [
      'info',
      'info',
      'info',
      'warn',
      'error',
      'debug',
    ]; // Weighted distribution
    const level = levels[Math.floor(Math.random() * levels.length)];
    const source =
      this.logSources[Math.floor(Math.random() * this.logSources.length)];

    let message =
      this.logMessages[level][
        Math.floor(Math.random() * this.logMessages[level].length)
      ];

    // Replace placeholders
    message = message
      .replace('{n}', Math.floor(Math.random() * 1000).toString())
      .replace('{data}', `#${Math.random().toString(36).substr(2, 6)}`)
      .replace('{key}', `cache_${Math.random().toString(36).substr(2, 4)}`)
      .replace('{old}', 'idle')
      .replace('{new}', 'syncing');

    this.addLog(level, message, source);
  }

  private addLog(level: LogLevel, message: string, source: string): void {
    const entry: LogEntry = {
      timestamp: new Date(),
      level,
      message,
      source,
    };

    this.logs.unshift(entry); // Add to beginning
    this.logCounts[level]++;

    // Keep only last N logs
    if (this.logs.length > this.maxLogs) {
      const removed = this.logs.pop();
      if (removed) {
        this.logCounts[removed.level]--;
      }
    }

    this.applyFilters();
    this.scrollToBottom();
  }

  toggleLevel(level: LogLevel): void {
    if (this.selectedLevels.has(level)) {
      this.selectedLevels.delete(level);
    } else {
      this.selectedLevels.add(level);
    }
    this.applyFilters();
  }

  onSearchChange(): void {
    this.applyFilters();
  }

  applyFilters(): void {
    this.filteredLogs = this.logs.filter((log) => {
      const levelMatch = this.selectedLevels.has(log.level);
      const searchMatch =
        !this.searchTerm ||
        log.message.toLowerCase().includes(this.searchTerm.toLowerCase()) ||
        log.source.toLowerCase().includes(this.searchTerm.toLowerCase());

      return levelMatch && searchMatch;
    });
  }

  clearLogs(): void {
    if (confirm('Are you sure you want to clear all logs?')) {
      this.logs = [];
      this.filteredLogs = [];
      this.logCounts = { info: 0, warn: 0, error: 0, debug: 0 };
    }
  }

  exportLogs(): void {
    const data = this.logs.map((log) => ({
      timestamp: log.timestamp.toISOString(),
      level: log.level,
      source: log.source,
      message: log.message,
    }));

    const blob = new Blob([JSON.stringify(data, null, 2)], {
      type: 'application/json',
    });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `logs-${new Date().toISOString()}.json`;
    link.click();
    URL.revokeObjectURL(url);
  }

  private scrollToBottom(): void {
    if (this.autoScroll && this.logContainer) {
      setTimeout(() => {
        const container = this.logContainer?.nativeElement;
        if (container) {
          container.scrollTop = container.scrollHeight;
        }
      }, 0);
    }
  }

  getLevelIcon(level: LogLevel): string {
    const icons = {
      info: 'ℹ️',
      warn: '⚠️',
      error: '❌',
      debug: '🔧',
    };
    return icons[level];
  }

  formatTimestamp(date: Date): string {
    return date.toLocaleTimeString('en-US', {
      hour12: false,
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      fractionalSecondDigits: 3,
    });
  }
}
