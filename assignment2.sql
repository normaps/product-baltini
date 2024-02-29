### QUERY FOR RETRIEVE SUGGESTIONS ####
SELECT pd.title as 'Group Title', COUNT(*) as 'Products Count', pd.updated_at as 'Updated At'
FROM product_duplicates pd
JOIN product_duplicate_lists pdl
ON pd.id = pdl.product_duplicate_id
GROUP BY pdl.product_duplicate_id

### ADJUSTMENT TABLE ####
# Set updated timestamp automatically
ALTER TABLE `product_duplicates`
MODIFY `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
MODIFY `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;

ALTER TABLE `product_duplicate_lists`
MODIFY `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
MODIFY `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;

# Add index
CREATE INDEX updatedAt_index
ON products(updated_at);

CREATE INDEX gender_category_updatedAt_index
ON products(gender, category, updated_at);
