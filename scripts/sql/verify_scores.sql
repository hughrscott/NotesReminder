-- Total imported historical scores
SELECT COUNT(*) AS total_scores
FROM lesson_note_scores_history;

-- Import coverage quality
SELECT
  COUNT(*) AS rows_imported,
  COUNT(DISTINCT lesson_id) AS distinct_lessons,
  SUM(CASE WHEN pike13_lesson_id IS NOT NULL AND TRIM(pike13_lesson_id) != '' THEN 1 ELSE 0 END) AS rows_with_pike13_id
FROM lesson_note_scores_history;

-- Score distribution (1-10 buckets)
SELECT
  CAST(score AS INTEGER) AS score_bucket,
  COUNT(*) AS notes
FROM lesson_note_scores_history
WHERE score IS NOT NULL
GROUP BY CAST(score AS INTEGER)
ORDER BY score_bucket;

-- Per-instructor monthly historical coverage
SELECT
  r.instructor_name,
  SUBSTR(r.lesson_date, 1, 7) AS month,
  COUNT(*) AS scored_lessons,
  ROUND(AVG(h.score), 2) AS avg_score
FROM lesson_note_scores_history h
JOIN reminders r ON r.lesson_id = h.lesson_id
GROUP BY r.instructor_name, SUBSTR(r.lesson_date, 1, 7)
ORDER BY month DESC, scored_lessons DESC, r.instructor_name;
