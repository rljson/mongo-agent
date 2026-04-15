import { CommonModule } from '@angular/common';
import { Component } from '@angular/core';

@Component({
  selector: 'app-settings',
  standalone: true,
  imports: [CommonModule],
  template: `
    <div class="settings">
      <h2>Settings</h2>
      <p class="placeholder-text">
        ⚙️ Settings view coming soon - Node configuration editor
      </p>
    </div>
  `,
  styles: [`
    .settings {
      max-width: 1400px;
      margin: 0 auto;
    }
    
    h2 {
      font-size: 2rem;
      font-weight: 600;
      color: #1a1a1a;
      margin-bottom: 1.5rem;
    }
    
    .placeholder-text {
      padding: 3rem;
      background: white;
      border-radius: 0.75rem;
      border: 2px dashed #e5e7eb;
      text-align: center;
      color: #6b7280;
      font-size: 1.125rem;
    }
  `],
})
export class SettingsComponent {}
