/**
 * Generate test conflicts for the UI
 * Run: npx tsx scripts/generate-test-conflicts.ts
 */

import { MongoClient } from 'mongodb';


async function generateConflicts() {
  const mongoUrl = process.env.MONGO_URI || 'mongodb://mongoa:27017';
  const client = new MongoClient(mongoUrl);

  try {
    await client.connect();
    console.log('✅ Connected to MongoDB');

    const db = client.db('syncdb');
    const conflictsCollection = db.collection('sync_conflicts');

    // Clear existing pending conflicts
    const deleted = await conflictsCollection.deleteMany({ status: 'pending' });
    console.log(
      `🗑️  Cleared ${deleted.deletedCount} existing pending conflicts`,
    );

    const now = Date.now();
    const conflicts = [
      // Conflict 1: Customer order update from different regions
      {
        conflictId: `conflict-${now}-001`,
        documentId: 'order-12345',
        collection: 'orders',
        database: 'rljson-sync',
        detectedAt: now - 180000, // 3 minutes ago
        status: 'pending',
        conflictType: 'concurrent-update',
        versions: [
          {
            documentId: 'order-12345',
            data: {
              _id: 'order-12345',
              customerId: 'customer-987',
              items: [
                { sku: 'LAPTOP-001', quantity: 2, price: 1299.99 },
                { sku: 'MOUSE-042', quantity: 2, price: 29.99 },
              ],
              total: 2659.96,
              status: 'processing',
              shippingAddress: '123 Main St, New York, NY 10001',
            },
            timestamp: now - 180000,
            nodeId: 'node-us-east',
            operationId: `op-${now}-001a`,
            operationType: 'update',
            stateHash: `hash-${Math.random().toString(36).substr(2, 8)}`,
            componentsHash: `comp-${Math.random().toString(36).substr(2, 8)}`,
          },
          {
            documentId: 'order-12345',
            data: {
              _id: 'order-12345',
              customerId: 'customer-987',
              items: [
                { sku: 'LAPTOP-001', quantity: 2, price: 1299.99 },
                { sku: 'MOUSE-042', quantity: 2, price: 29.99 },
                { sku: 'KEYBOARD-088', quantity: 1, price: 89.99 },
              ],
              total: 2749.95,
              status: 'confirmed',
              shippingAddress: '123 Main St, New York, NY 10001',
              notes: 'Customer requested expedited shipping',
            },
            timestamp: now - 178000,
            nodeId: 'node-us-west',
            operationId: `op-${now}-001b`,
            operationType: 'update',
            stateHash: `hash-${Math.random().toString(36).substr(2, 8)}`,
            componentsHash: `comp-${Math.random().toString(36).substr(2, 8)}`,
          },
        ],
      },

      // Conflict 2: Product inventory update
      {
        conflictId: `conflict-${now}-002`,
        documentId: 'product-SKU789',
        collection: 'products',
        database: 'rljson-sync',
        detectedAt: now - 90000, // 1.5 minutes ago
        status: 'pending',
        conflictType: 'concurrent-update',
        versions: [
          {
            documentId: 'product-SKU789',
            data: {
              _id: 'product-SKU789',
              name: 'Wireless Headphones Pro',
              sku: 'AUDIO-789',
              price: 249.99,
              stockQuantity: 45,
              category: 'Electronics',
              lastRestocked: now - 90000,
            },
            timestamp: now - 90000,
            nodeId: 'node-warehouse-a',
            operationId: `op-${now}-002a`,
            operationType: 'update',
            stateHash: `hash-${Math.random().toString(36).substr(2, 8)}`,
            componentsHash: `comp-${Math.random().toString(36).substr(2, 8)}`,
          },
          {
            documentId: 'product-SKU789',
            data: {
              _id: 'product-SKU789',
              name: 'Wireless Headphones Pro',
              sku: 'AUDIO-789',
              price: 229.99, // Price reduced
              stockQuantity: 42, // Different stock count
              category: 'Electronics',
              onSale: true,
              discount: 20,
            },
            timestamp: now - 88000,
            nodeId: 'node-warehouse-b',
            operationId: `op-${now}-002b`,
            operationType: 'update',
            stateHash: `hash-${Math.random().toString(36).substr(2, 8)}`,
            componentsHash: `comp-${Math.random().toString(36).substr(2, 8)}`,
          },
        ],
      },

      // Conflict 3: Employee record update
      {
        conflictId: `conflict-${now}-003`,
        documentId: 'employee-emma-456',
        collection: 'employees',
        database: 'rljson-sync',
        detectedAt: now - 45000, // 45 seconds ago
        status: 'pending',
        conflictType: 'concurrent-update',
        versions: [
          {
            documentId: 'employee-emma-456',
            data: {
              _id: 'employee-emma-456',
              firstName: 'Emma',
              lastName: 'Thompson',
              email: 'emma.thompson@company.com',
              department: 'Marketing',
              position: 'Marketing Manager',
              salary: 85000,
              startDate: '2020-03-15',
            },
            timestamp: now - 45000,
            nodeId: 'node-hr-system',
            operationId: `op-${now}-003a`,
            operationType: 'update',
            stateHash: `hash-${Math.random().toString(36).substr(2, 8)}`,
            componentsHash: `comp-${Math.random().toString(36).substr(2, 8)}`,
          },
          {
            documentId: 'employee-emma-456',
            data: {
              _id: 'employee-emma-456',
              firstName: 'Emma',
              lastName: 'Thompson',
              email: 'emma.thompson@company.com',
              department: 'Marketing',
              position: 'Senior Marketing Manager', // Promoted
              salary: 95000, // Raised
              startDate: '2020-03-15',
              certifications: ['Digital Marketing', 'SEO Specialist'],
            },
            timestamp: now - 43000,
            nodeId: 'node-payroll-system',
            operationId: `op-${now}-003b`,
            operationType: 'update',
            stateHash: `hash-${Math.random().toString(36).substr(2, 8)}`,
            componentsHash: `comp-${Math.random().toString(36).substr(2, 8)}`,
          },
        ],
      },

      // Conflict 4: Article content update
      {
        conflictId: `conflict-${now}-004`,
        documentId: 'article-tech-2024',
        collection: 'articles',
        database: 'rljson-sync',
        detectedAt: now - 20000, // 20 seconds ago
        status: 'pending',
        conflictType: 'concurrent-update',
        versions: [
          {
            documentId: 'article-tech-2024',
            data: {
              _id: 'article-tech-2024',
              title: 'The Future of AI in 2024',
              author: 'John Smith',
              content: 'Artificial intelligence continues to evolve...',
              tags: ['AI', 'Technology', 'Future'],
              published: true,
              views: 1250,
              lastModified: now - 20000,
            },
            timestamp: now - 20000,
            nodeId: 'node-cms-editor',
            operationId: `op-${now}-004a`,
            operationType: 'update',
            stateHash: `hash-${Math.random().toString(36).substr(2, 8)}`,
            componentsHash: `comp-${Math.random().toString(36).substr(2, 8)}`,
          },
          {
            documentId: 'article-tech-2024',
            data: {
              _id: 'article-tech-2024',
              title: 'The Future of AI in 2024 and Beyond',
              author: 'John Smith',
              co_author: 'Jane Doe',
              content:
                'Artificial intelligence continues to evolve rapidly, with new breakthroughs...',
              tags: ['AI', 'Technology', 'Future', 'Machine Learning'],
              published: true,
              views: 1245, // Different view count
              featured: true,
              lastModified: now - 18000,
            },
            timestamp: now - 18000,
            nodeId: 'node-cms-reviewer',
            operationId: `op-${now}-004b`,
            operationType: 'update',
            stateHash: `hash-${Math.random().toString(36).substr(2, 8)}`,
            componentsHash: `comp-${Math.random().toString(36).substr(2, 8)}`,
          },
        ],
      },

      // Conflict 5: User profile update with completely different changes
      {
        conflictId: `conflict-${now}-005`,
        documentId: 'user-alice-789',
        collection: 'users',
        database: 'rljson-sync',
        detectedAt: now - 5000, // 5 seconds ago
        status: 'pending',
        conflictType: 'concurrent-update',
        versions: [
          {
            documentId: 'user-alice-789',
            data: {
              _id: 'user-alice-789',
              username: 'alice.wonder',
              email: 'alice@example.com',
              firstName: 'Alice',
              lastName: 'Wonderland',
              phoneNumber: '+1-555-0123',
              preferences: {
                theme: 'dark',
                notifications: true,
              },
            },
            timestamp: now - 5000,
            nodeId: 'node-mobile-app',
            operationId: `op-${now}-005a`,
            operationType: 'update',
            stateHash: `hash-${Math.random().toString(36).substr(2, 8)}`,
            componentsHash: `comp-${Math.random().toString(36).substr(2, 8)}`,
          },
          {
            documentId: 'user-alice-789',
            data: {
              _id: 'user-alice-789',
              username: 'alice.wonder',
              email: 'alice.wonderland@newdomain.com', // Updated email
              firstName: 'Alice',
              lastName: 'Wonderland',
              phoneNumber: '+1-555-9999', // Updated phone
              preferences: {
                theme: 'light', // Different theme
                notifications: false, // Different setting
                language: 'en-US',
              },
              verified: true,
            },
            timestamp: now - 3000,
            nodeId: 'node-web-app',
            operationId: `op-${now}-005b`,
            operationType: 'update',
            stateHash: `hash-${Math.random().toString(36).substr(2, 8)}`,
            componentsHash: `comp-${Math.random().toString(36).substr(2, 8)}`,
          },
        ],
      },
    ];

    // Insert all conflicts
    const result = await conflictsCollection.insertMany(conflicts);
    console.log(`✅ Created ${result.insertedCount} fresh conflicts`);

    // Display summary
    console.log('\n📋 Conflicts created:');
    conflicts.forEach((c, i) => {
      const age = Math.floor((now - c.detectedAt) / 1000);
      console.log(
        `  ${i + 1}. ${c.conflictId.split('-').pop()} - ${c.collection}/${c.documentId} (${age}s old)`,
      );
    });

    console.log('\n🌐 View them at: http://localhost:4200/conflicts');
  } catch (error) {
    console.error('❌ Error:', error);
    throw error;
  } finally {
    await client.close();
  }
}

generateConflicts().catch(console.error);
