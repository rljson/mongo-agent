import { Routes } from '@angular/router';

import { ConflictListComponent } from './components/conflict-list/conflict-list.component';
import {
  ConflictResolverComponent
} from './components/conflict-resolver/conflict-resolver.component';


export const routes: Routes = [
  {
    path: '',
    component: ConflictListComponent,
  },
  {
    path: 'conflict/:id',
    component: ConflictResolverComponent,
  },
  {
    path: '**',
    redirectTo: '',
  },
];
