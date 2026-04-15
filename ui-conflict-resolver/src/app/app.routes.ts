import { Routes } from '@angular/router';

import { ConflictListComponent } from './components/conflict-list/conflict-list.component';
import {
  ConflictResolverComponent
} from './components/conflict-resolver/conflict-resolver.component';
import { DashboardComponent } from './features/dashboard/dashboard.component';
import { LogsComponent } from './features/logs/logs.component';
import { NetworkComponent } from './features/network/network.component';
import { SettingsComponent } from './features/settings/settings.component';
import { SyncComponent } from './features/sync/sync.component';
import { ShellComponent } from './layout/shell/shell.component';


export const routes: Routes = [
  {
    path: '',
    component: ShellComponent,
    children: [
      {
        path: '',
        redirectTo: 'dashboard',
        pathMatch: 'full',
      },
      {
        path: 'dashboard',
        component: DashboardComponent,
      },
      {
        path: 'network',
        component: NetworkComponent,
      },
      {
        path: 'sync',
        component: SyncComponent,
      },
      {
        path: 'conflicts',
        component: ConflictListComponent,
      },
      {
        path: 'conflicts/:id',
        component: ConflictResolverComponent,
      },
      {
        path: 'logs',
        component: LogsComponent,
      },
      {
        path: 'settings',
        component: SettingsComponent,
      },
    ],
  },
  {
    path: '**',
    redirectTo: 'dashboard',
  },
];
