import pandas as pd

# Load CSV with proper encoding
df = pd.read_csv(
    "data/Sample - Superstore.csv",
    parse_dates=["Order Date", "Ship Date"],
    encoding="ISO-8859-1"
)

# Quick nulls and uniques
print("Nulls per column:\n", df.isna().sum())
print("Unique values per column:\n", df.nunique())

# Max string length for categorical/text columns
text_cols = ["Order ID","Customer ID","Customer Name","Product ID","Product Name",
             "Category","Sub-Category","Region","City","State","Postal Code"]

for c in text_cols:
    if c in df.columns:
        maxlen = df[c].dropna().astype(str).map(len).max()
        print(c, "maxlen:", maxlen, "distinct:", df[c].nunique(), "nulls:", df[c].isna().sum())

# Numeric summary
num_cols = ["Sales","Profit","Discount","Quantity"]
for c in num_cols:
    if c in df.columns:
        s = pd.to_numeric(df[c].astype(str).str.replace('[$,]', '', regex=True), errors='coerce')
        print(c, "min:", s.min(), "max:", s.max(), "mean:", s.mean())

        # Estimate decimal length
        frac_len = s.dropna().apply(lambda x: len(str(x).split('.')[-1]) if '.' in str(x) else 0)
        print(c, "max decimal places:", frac_len.max())