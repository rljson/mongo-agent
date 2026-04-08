/**
 * Simple Backend API for Testing Conflict Resolution UI
 * This is a minimal version for demonstration purposes
 */

import express from 'express';
import cors from 'cors';
import { MongoClient, Db } from 'mongodb';

const app = express();
const port = 3000;

// Middleware
app.use(cors());
app.use(express.json());

let mongoClient: MongoClient;
let db: Db;

// Mock data for testing
const mockConflicts = new Map();

/**
 * Initialize MongoDB connection
 */
async function initializeMongoDB() {
  const mongoUrl = process.env.MONGO_URI || 'mongodb://mongoa:27017';
  mongoClient = new MongoClient(mongoUrl);
  await mongoClient.connect();
  
  db = mongoClient.db('syncdb');
  
  console.log('✅ MongoDB connected');
  
  // Create indexes for better query performance
  await db.collection('sync_conflicts').createIndex({ status: 1 });
  await db.collection('sync_conflicts').createIndex({ detectedAt: -1 });
  
  // Check for existing conflicts
  const conflictCount = await db.collection('sync_conflicts').countDocuments();
  console.log(`📊 Found ${conflictCount} conflicts in database`);
  
  // Create some sample conflicts for testing if none exist
  if (conflictCount === 0) {
    createSampleConflicts();
  }
}

/**
 * Create sample conflicts for UI testing
 */
function createSampleConflicts() {
  const conflict1 = {
    conflictId: 'conflict-001',
    documentId: 'user-alice-123',
    collection: 'users',
    database: 'rljson-sync',
    detectedAt: Date.now() - 300000, // 5 minutes ago
    status: 'pending',
    conflictType: 'concurrent-update',
    versions: [
      {
        documentId: 'user-alice-123',
        data: {
          _id: 'user-alice-123',
          name: 'Alice Johnson',
          age: 30,
          email: 'alice@example.com',
          status: 'active',
          department: 'Engineering'
        },
        timestamp: Date.now() - 300000,
        nodeId: 'node-a',
        operationId: 'op-001',
        operationType: 'update',
        stateHash: 'hash-a1b2c3',
        componentsHash: 'comp-d4e5f6'
      },
      {
        documentId: 'user-alice-123',
        data: {
          _id: 'user-alice-123',
          name: 'Alice Smith-Johnson',
          age: 31,
          email: 'alice.johnson@company.com',
          status: 'active',
          department: 'Engineering',
          role: 'Senior Developer'
        },
        timestamp: Date.now() - 299000,
        nodeId: 'node-b',
        operationId: 'op-002',
        operationType: 'update',
        stateHash: 'hash-g7h8i9',
        componentsHash: 'comp-j1k2l3'
      }
    ]
  };

  const conflict2 = {
    conflictId: 'conflict-002',
    documentId: 'user-bob-456',
    collection: 'users',
    database: 'rljson-sync',
    detectedAt: Date.now() - 120000, // 2 minutes ago
    status: 'pending',
    conflictType: 'concurrent-update',
    versions: [
      {
        documentId: 'user-bob-456',
        data: {
          _id: 'user-bob-456',
          name: 'Bob Wilson',
          age: 28,
          email: 'bob@example.com',
          status: 'inactive'
        },
        timestamp: Date.now() - 120000,
        nodeId: 'node-a',
        operationId: 'op-003',
        operationType: 'update',
        stateHash: 'hash-m4n5o6',
        componentsHash: 'comp-p7q8r9'
      },
      {
        documentId: 'user-bob-456',
        data: {
          _id: 'user-bob-456',
          name: 'Bob Wilson',
          age: 28,
          email: 'bob@example.com',
          status: 'active',
          lastLogin: Date.now()
        },
        timestamp: Date.now() - 118000,
        nodeId: 'node-c',
        operationId: 'op-004',
        operationType: 'update',
        stateHash: 'hash-s1t2u3',
        componentsHash: 'comp-v4w5x6'
      }
    ]
  };

  mockConflicts.set(conflict1.conflictId, conflict1);
  mockConflicts.set(conflict2.conflictId, conflict2);
  
  console.log(`✅ Created ${mockConflicts.size} sample conflicts for testing`);
}

// ==================== API ENDPOINTS ====================

/**
 * GET /api/conflicts
 */
app.get('/api/conflicts', async (req, res) => {
  try {
    const status = req.query.status as string | undefined;
    const query = status ? { status } : {};
    
    // Query both mock conflicts and real MongoDB conflicts
    const dbConflicts = await db.collection('sync_conflicts')
      .find(query)
      .sort({ detectedAt: -1 })
      .toArray();
    
    // Also include mock conflicts for demo purposes
    let mockConflictsList = Array.from(mockConflicts.values());
    if (status) {
      mockConflictsList = mockConflictsList.filter(c => c.status === status);
    }
    
    // Combine both sources
    const allConflicts = [...dbConflicts, ...mockConflictsList];
    
    res.json(allConflicts);
  } catch (error) {
    console.error('Error fetching conflicts:', error);
    res.status(500).json({ error: 'Failed to fetch conflicts' });
  }
});

/**
 * GET /api/conflicts/:id
 */
app.get('/api/conflicts/:id', async (req, res) => {
  try {
    // Try to find in MongoDB first
    const dbConflict = await db.collection('sync_conflicts').findOne({ 
      conflictId: req.params.id 
    });
    
    if (dbConflict) {
      return res.json(dbConflict);
    }
    
    // Fall back to mock data
    const mockConflict = mockConflicts.get(req.params.id);
    
    if (!mockConflict) {
      return res.status(404).json({ error: 'Conflict not found' });
    }
    
    res.json(mockConflict);
  } catch (error) {
    console.error('Error fetching conflict:', error);
    res.status(500).json({ error: 'Failed to fetch conflict' });
  }
});

/**
 * POST /api/conflicts/resolve
 */
app.post('/api/conflicts/resolve', async (req, res) => {
  try {
    const resolution = req.body;
    
    // Try to find and update in MongoDB first
    const dbConflict = await db.collection('sync_conflicts').findOne({ 
      conflictId: resolution.conflictId 
    });
    
    if (dbConflict) {
      // Apply the resolved document back to the collection
      const resolvedDoc = resolution.mergedDocument || resolution.selectedVersion?.data;
      
      if (resolvedDoc) {
        const collectionName = dbConflict.collection || 'articles';
        const collection = db.collection(collectionName);
        
        // Update the document in the collection with the resolved version
        await collection.updateOne(
          { _id: dbConflict.documentId },
          { 
            $set: {
              ...resolvedDoc,
              _resolvedFrom: resolution.conflictId,
              _resolvedAt: new Date(),
              _resolvedBy: resolution.resolutionType
            }
          },
          { upsert: true }
        );
        
        console.log(`✅ Applied resolved document to ${collectionName} collection`);
      } else {
        console.warn(`⚠️  No resolved document provided in resolution`);
      }
      
      // Update conflict status
      await db.collection('sync_conflicts').updateOne(
        { conflictId: resolution.conflictId },
        { 
          $set: { 
            status: 'resolved',
            resolution: resolution,
            resolvedAt: Date.now()
          } 
        }
      );
      
      console.log(`✅ Resolved conflict: ${resolution.conflictId} using ${resolution.resolutionType}`);
      
      return res.json({ 
        success: true, 
        message: 'Conflict resolved and applied successfully' 
      });
    }
    
    // Fall back to mock data
    const conflict = mockConflicts.get(resolution.conflictId);
    
    if (!conflict) {
      return res.status(404).json({ 
        success: false, 
        error: 'Conflict not found' 
      });
    }
    
    // Mark as resolved
    conflict.status = 'resolved';
    conflict.resolution = resolution;
    mockConflicts.set(conflict.conflictId, conflict);
    
    console.log(`✅ Resolved conflict: ${resolution.conflictId} using ${resolution.resolutionType}`);
    
    res.json({ 
      success: true, 
      message: 'Conflict resolved successfully' 
    });
  } catch (error) {
    console.error('Error resolving conflict:', error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to resolve conflict'
    });
  }
});

/**
 * GET /api/agents/status
 */
app.get('/api/agents/status', (req, res) => {
  try {
    const statuses = [
      {
        nodeId: 'node-a',
        lastSync: Date.now() - 30000,
        stateHash: 'hash-current-a',
        pendingOperations: 2,
        isOnline: true
      },
      {
        nodeId: 'node-b',
        lastSync: Date.now() - 45000,
        stateHash: 'hash-current-b',
        pendingOperations: 1,
        isOnline: true
      },
      {
        nodeId: 'node-c',
        lastSync: Date.now() - 60000,
        stateHash: 'hash-current-c',
        pendingOperations: 0,
        isOnline: true
      }
    ];
    
    res.json(statuses);
  } catch (error) {
    console.error('Error fetching agent status:', error);
    res.status(500).json({ error: 'Failed to fetch agent status' });
  }
});

/**
 * GET /api/documents/:id/history
 */
app.get('/api/documents/:id/history', async (req, res) => {
  try {
    const documentId = req.params.id;
    
    // Return mock history
    const history = [
      {
        documentId,
        operationType: 'insert',
        timestamp: Date.now() - 500000,
        nodeId: 'node-a',
        operationId: 'op-000'
      },
      {
        documentId,
        operationType: 'update',
        timestamp: Date.now() - 300000,
        nodeId: 'node-a',
        operationId: 'op-001'
      },
      {
        documentId,
        operationType: 'update',
        timestamp: Date.now() - 299000,
        nodeId: 'node-b',
        operationId: 'op-002'
      }
    ];
    
    res.json(history);
  } catch (error) {
    console.error('Error fetching document history:', error);
    res.status(500).json({ error: 'Failed to fetch document history' });
  }
});

/**
 * GET /api/conflicts/:id/verify-chain
 */
app.get('/api/conflicts/:id/verify-chain', (req, res) => {
  try {
    const conflict = mockConflicts.get(req.params.id);
    
    if (!conflict) {
      return res.status(404).json({ error: 'Conflict not found' });
    }
    
    // Mock verification - in real implementation, verify hash chain
    const verificationResults = conflict.versions.map((v: any) => ({
      nodeId: v.nodeId,
      operationId: v.operationId,
      valid: true
    }));
    
    res.json({
      valid: true,
      details: verificationResults
    });
  } catch (error) {
    console.error('Error verifying hash chain:', error);
    res.status(500).json({ error: 'Failed to verify hash chain' });
  }
});

/**
 * POST /api/sync/trigger
 */
app.post('/api/sync/trigger', (req, res) => {
  try {
    console.log('🔄 Sync triggered manually');
    res.json({ success: true });
  } catch (error) {
    console.error('Error triggering sync:', error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to trigger sync' 
    });
  }
});

// ==================== START SERVER ====================

async function startServer() {
  try {
    await initializeMongoDB();
    
    app.listen(port, () => {
      console.log('');
      console.log('='.repeat(60));
      console.log(`🚀 RLJSON Conflict Resolution API running`);
      console.log('='.repeat(60));
      console.log(`📍 Server:    http://localhost:${port}`);
      console.log(`📊 MongoDB:   mongodb://localhost:27017`);
      console.log('');
      console.log('📝 Available Endpoints:');
      console.log(`   GET  http://localhost:${port}/api/conflicts`);
      console.log(`   GET  http://localhost:${port}/api/conflicts/:id`);
      console.log(`   POST http://localhost:${port}/api/conflicts/resolve`);
      console.log(`   GET  http://localhost:${port}/api/agents/status`);
      console.log(`   GET  http://localhost:${port}/api/documents/:id/history`);
      console.log(`   GET  http://localhost:${port}/api/conflicts/:id/verify-chain`);
      console.log(`   POST http://localhost:${port}/api/sync/trigger`);
      console.log('='.repeat(60));
      console.log('');
      console.log('💡 Tip: Open http://localhost:4200 for the UI');
      console.log('');
    });
    
  } catch (error) {
    console.error('❌ Failed to start server:', error);
    process.exit(1);
  }
}

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n🛑 Shutting down gracefully...');
  
  if (mongoClient) {
    await mongoClient.close();
  }
  
  process.exit(0);
});

startServer();
