#!/bin/bash
# Seed test data into MongoDB for E2E tests
set -e

echo "🌱 Seeding test data into MongoDB..."
echo ""

# Number of test articles to create
NUM_ARTICLES=${1:-100}

echo "📊 Creating $NUM_ARTICLES test articles in MongoDB A..."

docker compose exec -T mongoa mongosh syncdb --quiet --eval "
// Clear existing test data
db.articles.deleteMany({ testData: true });

// Generate test articles
const articles = [];
const categories = ['Technology', 'Science', 'Business', 'Health', 'Sports', 'Entertainment'];
const authors = ['Alice', 'Bob', 'Carol', 'David', 'Eve', 'Frank'];

for (let i = 0; i < $NUM_ARTICLES; i++) {
  articles.push({
    testData: true,
    title: \`Article \${i + 1}: \${categories[i % categories.length]} News\`,
    author: authors[i % authors.length],
    category: categories[i % categories.length],
    content: \`This is test article number \${i + 1}. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.\`,
    publishedAt: new Date(Date.now() - Math.random() * 365 * 24 * 60 * 60 * 1000),
    viewCount: Math.floor(Math.random() * 10000),
    likeCount: Math.floor(Math.random() * 1000),
    featured: Math.random() > 0.8,
    tags: categories.slice(0, Math.floor(Math.random() * 3) + 1),
    metadata: {
      readTime: Math.floor(Math.random() * 20) + 1,
      wordCount: Math.floor(Math.random() * 2000) + 100
    }
  });
}

// Insert in batches
const batchSize = 100;
let inserted = 0;
for (let i = 0; i < articles.length; i += batchSize) {
  const batch = articles.slice(i, i + batchSize);
  const result = db.articles.insertMany(batch);
  inserted += result.insertedIds ? Object.keys(result.insertedIds).length : 0;
}

print('Inserted ' + inserted + ' articles');

// Show stats
const total = db.articles.countDocuments();
print('Total articles in database: ' + total);
"

echo ""
echo "✓ Seeded $NUM_ARTICLES articles into MongoDB A"

echo ""
echo "📊 Current database status:"
echo ""

echo "MongoDB A:"
COUNTA=$(docker compose exec -T mongoa mongosh syncdb --quiet --eval 'db.articles.countDocuments()' | tail -1)
echo "  Total articles: $COUNTA"

echo ""
echo "MongoDB B:"
COUNTB=$(docker compose exec -T mongob mongosh syncdb --quiet --eval 'db.articles.countDocuments()' | tail -1)
echo "  Total articles: $COUNTB"

echo ""
echo "💡 Tip: Wait a few seconds for sync, then check MongoDB B:"
echo "   docker compose exec mongob mongosh syncdb --eval 'db.articles.countDocuments()'"
echo ""
echo "🧪 Now you can run E2E tests:"
echo "   npx tsx test-integrity-hash.ts"
echo "   npx tsx test-state-hash.ts"
echo "   ./test-sync.sh"
