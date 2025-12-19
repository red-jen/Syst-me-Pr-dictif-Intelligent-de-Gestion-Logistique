
Columns & plain examples

Order Item Product Price — unit price before discount.
Example: price = 100.00 (currency units).

Order Item Discount — absolute money discount applied to that item/line.
Example: discount = 20.00 → you paid 80.00 if qty=1.

Order Item Discount Rate — discount as a fraction or percent.
Example interpretations:

If stored as fraction: 0.20 → 20% off.
If stored as percent: 20 → 20% off.
(discount_rate = discount / product_price)
Order Item Profit Ratio — profit divided by revenue for that item/line (profit margin).
Example: revenue = price * qty = 100 * 2 = 200; profit = 50 → profit_ratio = 50 / 200 = 0.25 (25% margin).

Why normalize (discount_ratio) and difference between fields

Absolute discount (20) is ambiguous: $20 off is big for a $40 product (50%) but small for $400 product (5%).
discount_ratio = Order Item Discount / Order Item Product Price gives a comparable percentage across SKUs. Use that in models instead of raw discount.
How these help predict Late_delivery_risk (intuition)

Large discount_rate or promotional orders → may follow different fulfillment rules (outsourced, bulk), possibly higher or lower lateness. Model can learn that association.
High product price or high order value (price * qty) → often treated as high-priority (faster handling) → lower late risk. So include order_amount = price * qty.
Low profit_ratio (thin margin) → business might deprioritize expensive logistics for low-margin items → could increase late risk. Or conversely high-margin customers get priority.
Quantity affects packaging/processing time; many items may need split shipments → increased delay risk.




why dicount_ratio calcul :

Reasons (concise) why you might recompute instead of using "Order Item Discount Rate":

Data trust/audit: recomputing derives the value from raw fields (price, discount) so you can prove the formula and detect upstream data errors.
Consistency: ensures the same formula is used everywhere (no hidden pre-processing in the source CSV).
Edge handling: recompute lets you explicitly guard divides-by-zero, nulls, negative prices, or apply business rules (clamp, floor, cap).
Reproducibility: easier to reproduce results from raw inputs when sharing code or retraining.
Precision control: division reveals floating-point noise; you can then round/cast to desired precision centrally rather than relying on inconsistent stored formatting.
Instrumentation: recomputing allows logging/counting mismatches to catch bad rows or ETL issues.