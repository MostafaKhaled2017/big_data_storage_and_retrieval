// Q3 (MongoDB): Full-text search on products.category_code by keywords from Q2 result.
// Ensure text index once.
db.products.createIndex(
  { category_code: "text", brand: "text" },
  { name: "idx_products_fulltext", default_language: "none" }
);

// Build keywords from category_code of Q2 top products (top 3 keywords).
// Use q2.js output list (or repeat the Q2 aggregation), then:
// 1) split category_code by non-alphanumeric chars
// 2) count token frequency
// 3) keep top-3 tokens with length > 2

// Example text search for one keyword:
// db.products.find(
//   { $text: { $search: "electronics" } },
//   { score: { $meta: "textScore" }, category_code: 1, brand: 1 }
// ).sort({ score: { $meta: "textScore" } })
