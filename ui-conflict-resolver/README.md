# RLJSON Conflict Resolver UI

Angular-based UI for resolving merge conflicts in distributed MongoDB synchronization.

## Features

- 🔍 **Conflict Detection**: Automatically detect when multiple nodes edit the same document
- 📊 **Visual Diff**: Side-by-side comparison of conflicting versions
- ⏱️ **Timestamp Tracking**: Show when each change was made and by which node
- 🎯 **Smart Resolution**: Choose between versions or create merged version
- 🔗 **Blockchain Verification**: Verify operation chain integrity
- 📝 **Audit Trail**: Track all conflict resolutions

## Architecture Integration

This UI integrates with the RLJSON MongoDB synchronization system:

```
┌─────────────┐         ┌─────────────┐         ┌─────────────┐
│  MongoDB A  │◄───────►│   Agent A   │◄───────►│     Hub     │
└─────────────┘         └─────────────┘         └─────────────┘
                                                        ▲
                                                        │
                                                        ▼
                        ┌─────────────────────┐
                        │  Conflict Resolver  │  ◄── Angular UI
                        │        UI           │
                        └─────────────────────┘
                                ▲
                                │
                        Detect conflicts,
                        show diffs,
                        apply resolution
```

## Installation

```bash
cd ui-conflict-resolver
npm install
npm start
```

## Usage

1. **Monitor Conflicts**: UI automatically polls for conflicts
2. **Review Changes**: See side-by-side diff of conflicting versions
3. **Choose Resolution**:
   - Keep local version
   - Keep remote version
   - Manual merge
   - Field-by-field selection
4. **Apply & Verify**: Apply resolution and verify with state hash

## Development

```bash
# Install dependencies
npm install

# Run development server
npm start

# Run tests
npm test

# Build for production
npm run build
```

## Components

- `ConflictListComponent`: List all detected conflicts
- `ConflictDetailComponent`: Show detailed diff view
- `ConflictResolverComponent`: Interface for resolving conflicts
- `DiffViewerComponent`: Visual diff renderer
- `FieldMergeComponent`: Field-by-field merge selector

## Services

- `ConflictDetectionService`: Poll for conflicts from backend
- `ConflictResolutionService`: Apply resolution choices
- `SyncApiService`: Communicate with RLJSON sync agents
- `DiffService`: Calculate differences between versions
