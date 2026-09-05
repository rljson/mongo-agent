import { MongoClient } from 'mongodb';

async function check() {
  const client = new MongoClient('mongodb://localhost:27017');
  await client.connect();
  const count = await client
    .db('test_articles_performance')
    .collection('cd_articles')
    .countDocuments();
  console.log(`✓ Articles in test database: ${count.toLocaleString()}`);

  const countB = await client
    .db('test_articles_performance_nodeB')
    .collection('cd_articles')
    .countDocuments();
  console.log(`✓ Articles in Node B: ${countB.toLocaleString()}`);

  const countC = await client
    .db('test_articles_performance_nodeC')
    .collection('cd_articles')
    .countDocuments();
  console.log(`✓ Articles in Node C: ${countC.toLocaleString()}`);

  await client.close();
}

check().catch(console.error);
