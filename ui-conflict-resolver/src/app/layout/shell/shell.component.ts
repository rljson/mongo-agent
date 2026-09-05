import { CommonModule } from '@angular/common';
import { Component } from '@angular/core';
import { RouterLink, RouterLinkActive, RouterOutlet } from '@angular/router';


@Component({
  selector: 'app-shell',
  standalone: true,
  imports: [CommonModule, RouterOutlet, RouterLink, RouterLinkActive],
  templateUrl: './shell.component.html',
  styleUrls: ['./shell.component.scss'],
})
export class ShellComponent {
  nodeId = 'NodeA-abc123'; // TODO: Get from API
  nodeRole = 'Hub'; // TODO: Get from API

  navItems = [
    { path: '/dashboard', label: 'Dashboard', icon: '📊' },
    { path: '/network', label: 'Network', icon: '🌐' },
    { path: '/sync', label: 'Sync', icon: '🔄' },
    { path: '/conflicts', label: 'Conflicts', icon: '⚠️' },
    { path: '/logs', label: 'Logs', icon: '📋' },
    { path: '/settings', label: 'Settings', icon: '⚙️' },
  ];
}
