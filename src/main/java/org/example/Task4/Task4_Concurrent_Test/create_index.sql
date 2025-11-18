--高并发测试

-- 测试数据库功能
SELECT * FROM review WHERE rating >= 4.5 ORDER BY date_submit DESC LIMIT 10;

-- 对常见查询的属性建立索引
-- recipe's author
CREATE INDEX idx_recipe_author ON recipe(author_id);
-- review's recipe and author
CREATE INDEX idx_review_recipe ON review(recipe_id);
CREATE INDEX idx_review_author ON review(author_id);
-- favors_recipe
CREATE INDEX idx_favor_author ON favors_recipe(author_id);
CREATE INDEX idx_favor_recipe ON favors_recipe(recipe_id);
-- likes_review
CREATE INDEX idx_like_author ON likes_review(author_id);
CREATE INDEX idx_like_review ON likes_review(review_id);
-- has_category
CREATE INDEX idx_has_category_category ON has_category(category_id);
CREATE INDEX idx_has_category_recipe ON has_category(recipe_id);
-- has_keyword
CREATE INDEX idx_has_keyword_keyword ON has_keyword(keyword_id);
CREATE INDEX idx_has_keyword_recipe ON has_keyword(recipe_id);
-- has_ingredient
CREATE INDEX idx_has_ingr_ing ON has_ingredient(ingredient_id);
CREATE INDEX idx_has_ingr_recipe ON has_ingredient(recipe_id);
-- instruction
CREATE INDEX idx_instr_recipe ON instruction(recipe_id);

-- 测试索引是否生效
EXPLAIN ANALYZE SELECT * FROM review WHERE recipe_id = 123;

-- 测试完成后，删除索引（可选）
DROP INDEX IF EXISTS idx_recipe_author;
DROP INDEX IF EXISTS idx_review_recipe;
DROP INDEX IF EXISTS idx_review_author;
DROP INDEX IF EXISTS idx_favor_author;
DROP INDEX IF EXISTS idx_favor_recipe;
DROP INDEX IF EXISTS idx_like_author;
DROP INDEX IF EXISTS idx_like_review;
DROP INDEX IF EXISTS idx_has_category_category;
DROP INDEX IF EXISTS idx_has_category_recipe;
DROP INDEX IF EXISTS idx_has_keyword_keyword;
DROP INDEX IF EXISTS idx_has_keyword_recipe;
DROP INDEX IF EXISTS idx_has_ingr_ing;
DROP INDEX IF EXISTS idx_has_ingr_recipe;
DROP INDEX IF EXISTS idx_instr_recipe;