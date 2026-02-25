"""
Sales Data Normalizer for German Kraft Brewing Limited.
Standardizes categories, extracts modifiers, merges duplicates, and organizes sales data.

NOTE – Modular refactor
-----------------------
The core logic now lives in Scripts/normalizer.py as part of the Dines → Polaris
splitting pipeline.  This file is kept as a backward-compatible standalone entry
point; it delegates entirely to the Scripts module.

To run the full pipeline (all four output files) use:
    python main.py

To run only the standalone normalizer (legacy behaviour):
    python normalizer.py
"""

from collections import defaultdict
import re
import csv
import sys
from pathlib import Path

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).parent))

from .Scripts.normalizer import (  # noqa: F401  (re-export for any external imports)
    CATEGORY_MAPPING,
    WINE_SIZE_PATTERNS,
    SORT_COLUMNS,
    MODIFIER_SORT_ORDER,
    PRODUCT_SORT_ORDER,
    MIXER_SORT_ORDER,
    MIXER_NORMALIZATION,
    MODIFIER_NORMALIZATION,
    PRODUCT_NORMALIZATION,
    PRODUCT_CATEGORY_CORRECTIONS,
    PRODUCTS_TO_SWAP,
    NUMERIC_FIELDS,
    OUTPUT_COLUMNS,
    clean_numeric_value,
    format_numeric_value,
    normalize_sales as process_csv_fn,
    process_csv,
    main,
)


# Category Standardization Dictionary
# Add or modify mappings here to update category standardization rules
CATEGORY_MAPPING = {
    # Draught categories
    "Draught": "Draught Beer",
    "Draught Beer": "Draught Beer",

    # Spirits categories
    "Spirits (GB/GH)": "Spirits",
    "Spirits (DI/CE)": "Spirits",

    # Gin categories
    "Gin": "Gin",

    # Wine categories
    "Wines": "Wines",

    # Guest Beer/Cider
    "Guest Beer / Cider": "Guest Draughts",

    # Cocktail categories
    "Signature Cocktails": "Signature Cocktails",
    "Classic Cocktails": "Classic Cocktails",
    "Cocktails (GB/GH)": "Cocktails",
    "Cocktails (DI/CE)": "Cocktails",

    # Non-alcoholic
    "Non-alcoholic Cocktails": "Non-alcoholic Cocktails",

    # Spritz
    "Spritz": "Spritz",

    # Bottled items
    "Bottled Beer / Lager": "Bottled Beers",
    "Bottled Softs": "Bottled Softs",

    # Minerals
    "Minerals": "Minerals",

    # Shot Rack
    "Shot Rack": "Shot Rack",

    # Promotions
    "Beer & Meal Promo": "Promotions",
    "Student Deals": "Promotions"
}

# Wine size patterns to extract from product names
WINE_SIZE_PATTERNS = {
    r'\s+250ml$': '250ml',
    r'\s+175ml$': '175ml',
    r'\s+125ml$': '125ml',
    r'\s+Btl$': 'Bottle',
}

# Multi-column sorting configuration
# Format: [(column_name, sort_type), ...]
# sort_type can be 'alpha' (alphabetical), 'numeric', or 'custom'
SORT_COLUMNS = [
    ('Category', 'alpha'),
    ('Modifiers', 'custom'),   # Changed to custom for special Modifier sorting
    ('Product', 'custom'),    # Changed to custom for special Product sorting
    ('Mixer', 'custom'),      # Custom for special Mixer sorting
    ('Qty', 'numeric'),
]

# Custom sorting order for Modifier column
# Lower priority number = appears first in sorted output
MODIFIER_SORT_ORDER = [
    'Pint',
    'Half',
    'Mass',
    'Shandy',
    'Shandy Half',
    'Top',
    'Top Half',
    'Single',
    'Double',
    'Bottle',
    '250ml',
    '175ml',
    '125ml',
]

# Custom sorting order for Product column
# Lower priority number = appears first in sorted output
PRODUCT_SORT_ORDER = [
    'Heinr Zwickel',
    'Heidi Helles',
    'Siggi',
    'Lotte Weissb',
    'Fritz',
    'Schwarzbier',
]

# Custom sorting order for Mixer column
# Items are sorted by priority groups first, then alphabetically within each group
# Lower priority number = appears first in sorted output
MIXER_SORT_ORDER = [
    'NO Mixer',           # Priority 0
    'POSTMIX',            # Priority 1 - all POSTMIX items
    'F&S',                # Priority 2 - Franklin & Sons abbreviated
    'FEVERTREE',          # Priority 3
    'FRITZ',              # Priority 4
]

# Global value normalization/corrections
# Apply these corrections to standardize inconsistent naming across data
# Format: {incorrect_value: correct_value}
MIXER_NORMALIZATION = {
    "FRANKLIN & Sons Cola": "F&S Cola",
    "FRANKLIN & Sons Ginger Ale": "F&S Ginger Ale",
    "FRANKLIN & Sons Ginger Beer": "F&S Ginger Beer",
    "FRANKLIN & Sons Indian Tonic": "F&S Indian Tonic",
    "FRANKLIN & Sons Lemonade": "F&S Lemonade",
    "FRANKLIN & Sons Soda Water": "F&S Soda Water",
    "DIET Cola": "POSTMIX Diet Cola",
    "FEVERTREE Aromatic": "Fevertree Aromatic Tonic",
    "FEVERTREE Elderflower": "Fevertree Elderflower Tonic",
    # Add more mixer normalizations here as needed
}

MODIFIER_NORMALIZATION = {
    # Add modifier normalizations here if needed in the future
    "Mezcal Classic Margarita": "Margarita",
    "Half Shandy": "Shandy Half",
    "Half Top": "Top Half"
}

PRODUCT_NORMALIZATION = {
    # Add product normalizations here if needed in the future
    "Bero Kingston Golden Pils (NON-ALC)": "Bero Golden Pils",
    "Bero Kingston Hazy Ipa": "Bero Hazy IPA",
    "Big Drop Pine Trail Can": "Big Drop Pine Trail",
    "Big Drop Reef Point Can": "Big Drop Reef Point",
    "F&s Ginger Ale": "F&S Ginger Ale",
    "F&s Ginger Beer": "F&S Ginger Beer",
    "F&s Indian Tonic": "F&S Indian Tonic",
    "F&s Light Tonic": "F&S Light Tonic",
    "F&s Lemonade": "F&S Lemonade",
    "F&s Soda Water": "F&S Soda Water",
    "Franklin & Sons Cola": "F&S Cola",
}

# Product-specific category corrections
# Use this to fix miscategorized products in the source data
PRODUCT_CATEGORY_CORRECTIONS = {
    "Bero Kingston Hazy Ipa": "Bottled Beers",
    "Bero Kingston Golden Pils (NON-ALC)": "Bottled Beers",
    "Big Drop Pine Trail Can": "Bottled Beers",
    "Big Drop Reef Point Can": "Bottled Beers",
    "Purity Session Ipa": "Guest Draughts",
    "Caple Road": "Guest Draughts",
    "£5 Pint": "Promotions",
    "Wrap & Pint": "Promotions"
}

# Products that are actually modifiers (need Product/Modifier swap)
# These entries have the modifier in the Product field and product in the Modifier field
PRODUCTS_TO_SWAP = {
    "£5 Pint",
    "Wrap & Pint",
    "Beer Pitcher",
    "Beer Flight",
    "Pre-sold Mass",
    "Pre-sold Pint",
    "Margarita",
    "Mojito",
    "Virgin Mojito",
    "Virgin Collins",
    "Daiquiri"
}

# Numeric fields to aggregate when merging duplicates
NUMERIC_FIELDS = ['Qty', 'Item Value', 'Modifier Value',
                  'Gross Product Sales', 'Cost Price', 'Gross Profit']

# Output column order
OUTPUT_COLUMNS = ['Category', 'Modifiers', 'Product', 'Mixer', 'Qty',
                  'Item Value', 'Modifier Value', 'Gross Product Sales',
                  'Cost Price', 'Gross Profit']

# Constants for sorting
SORT_EMPTY_VALUE = 'zzzzz'
SORT_EMPTY_PRIORITY = 999
SORT_TYPE_ALPHA = 'alpha'
SORT_TYPE_NUMERIC = 'numeric'
SORT_TYPE_CUSTOM = 'custom'

# Column name mapping for custom sort
CUSTOM_SORT_MAPPING = {
    'Mixer': MIXER_SORT_ORDER,
    'Modifiers': MODIFIER_SORT_ORDER,
    'Product': PRODUCT_SORT_ORDER,
}


def clean_numeric_value(value):
    """
    Convert a string value to float, handling commas and empty values.

    Args:
        value: String or numeric value

    Returns:
        float: Cleaned numeric value
    """
    try:
        clean_value = str(value).replace(',', '')
        return float(clean_value) if clean_value else 0.0
    except (ValueError, AttributeError):
        return 0.0


def format_numeric_value(value, as_integer=False):
    """
    Format a numeric value as a string with commas.

    Args:
        value: Numeric value
        as_integer: Whether to format as integer

    Returns:
        str: Formatted value
    """
    if as_integer:
        return str(int(value))
    return f"{value:,.2f}"


def standardize_category(category):
    """
    Standardize a category name using the mapping dictionary.

    Args:
        category (str): Original category name

    Returns:
        str: Standardized category name
    """
    return CATEGORY_MAPPING.get(category, category)


def apply_product_category_correction(product_name, category):
    """
    Apply product-specific category corrections for miscategorized items.

    Args:
        product_name (str): Product name
        category (str): Current category

    Returns:
        str: Corrected category if product is in corrections dict, otherwise original
    """
    return PRODUCT_CATEGORY_CORRECTIONS.get(product_name, category)


def normalize_value(value, normalization_dict):
    """
    Normalize a value using a normalization dictionary.
    This standardizes inconsistent naming across the dataset.

    Args:
        value (str): Original value
        normalization_dict (dict): Dictionary of corrections

    Returns:
        str: Normalized value if in dict, otherwise original
    """
    if not value:
        return value
    return normalization_dict.get(value, value)


def normalize_row_values(product, modifier, mixer):
    """
    Apply global normalization to product, modifier, and mixer values.
    This ensures consistent naming before merging duplicates.

    Args:
        product (str): Product name
        modifier (str): Modifier value
        mixer (str): Mixer value

    Returns:
        tuple: (normalized_product, normalized_modifier, normalized_mixer)
    """
    normalized_product = normalize_value(product, PRODUCT_NORMALIZATION)
    normalized_modifier = normalize_value(modifier, MODIFIER_NORMALIZATION)
    normalized_mixer = normalize_value(mixer, MIXER_NORMALIZATION)

    return normalized_product, normalized_modifier, normalized_mixer


def swap_product_modifier(product_name, modifier):
    """
    Swap product and modifier values for entries where the product is actually a modifier.

    Args:
        product_name (str): Current product name
        modifier (str): Current modifier value

    Returns:
        tuple: (corrected_product, corrected_modifier)
    """
    if product_name in PRODUCTS_TO_SWAP:
        # Swap: product becomes modifier, modifier becomes product
        return modifier, product_name
    return product_name, modifier


def standardize_wine_product(product_name, current_modifier, category):
    """
    Extract wine size from product name and move it to modifiers.

    Args:
        product_name (str): Original product name
        current_modifier (str): Current modifier value
        category (str): Product category

    Returns:
        tuple: (cleaned_product_name, updated_modifier)
    """
    if category.lower() != 'wines':
        return product_name, current_modifier

    for pattern, size in WINE_SIZE_PATTERNS.items():
        if re.search(pattern, product_name, re.IGNORECASE):
            cleaned_name = re.sub(pattern, '', product_name,
                                  flags=re.IGNORECASE).strip()

            if current_modifier and current_modifier.strip():
                updated_modifier = f"{current_modifier},{size}"
            else:
                updated_modifier = size

            return cleaned_name, updated_modifier

    return product_name, current_modifier


def extract_mixer_from_modifier(modifier, category):
    """
    Extract mixer information from the modifier field for spirits/gin.

    Args:
        modifier (str): Original modifier value (may contain size and mixer)
        category (str): Product category

    Returns:
        tuple: (updated_modifier, mixer)
    """
    mixer_categories = ['spirits', 'gin']

    if not any(cat in category.lower() for cat in mixer_categories):
        return modifier, ""

    if not modifier or not modifier.strip():
        return modifier, ""

    parts = modifier.split(',')

    if len(parts) == 1:
        return modifier, ""

    modifier_part = parts[0].strip()
    mixer_part = ','.join(parts[1:]).strip()

    return modifier_part, mixer_part


def merge_duplicate_entries(data_rows):
    """
    Merge duplicate entries with the same Category, Modifiers, Product, and Mixer.
    Aggregate numeric fields (Qty, values, sales, etc.)

    Args:
        data_rows (list): List of dictionaries representing CSV rows

    Returns:
        tuple: (merged_rows, duplicates_merged_count)
    """
    def create_key(row):
        """Create a tuple key from row identifiers."""
        return (
            row.get('Category', ''),
            row.get('Modifiers', ''),
            row.get('Product', ''),
            row.get('Mixer', '')
        )

    # Group data by key and track counts
    grouped = defaultdict(
        lambda: {'numeric': {field: 0.0 for field in NUMERIC_FIELDS}, 'count': 0})

    for row in data_rows:
        key = create_key(row)
        grouped[key]['count'] += 1

        # Aggregate numeric values
        for field in NUMERIC_FIELDS:
            value = row.get(field, '0')
            grouped[key]['numeric'][field] += clean_numeric_value(value)

    # Calculate duplicates merged
    duplicates_merged = sum(
        data['count'] - 1 for data in grouped.values() if data['count'] > 1)

    # Convert back to list of rows
    merged_rows = []
    for key, data in grouped.items():
        category, modifiers, product, mixer = key
        aggregated = data['numeric']

        merged_row = {
            'Category': category,
            'Modifiers': modifiers,
            'Product': product,
            'Mixer': mixer,
            'Qty': format_numeric_value(aggregated['Qty'], as_integer=True),
            'Item Value': format_numeric_value(aggregated['Item Value']),
            'Modifier Value': format_numeric_value(aggregated['Modifier Value']),
            'Gross Product Sales': format_numeric_value(aggregated['Gross Product Sales']),
            'Cost Price': format_numeric_value(aggregated['Cost Price']),
            'Gross Profit': format_numeric_value(aggregated['Gross Profit'])
        }

        merged_rows.append(merged_row)

    return merged_rows, duplicates_merged


def get_custom_sort_key(value, sort_order_list):
    """
    Generate a sort key for values based on custom priority order.

    Args:
        value (str): Value to sort
        sort_order_list (list): Priority order list

    Returns:
        tuple: (priority, alphabetic_value) for sorting
    """
    if not value or not str(value).strip():
        return (SORT_EMPTY_PRIORITY, SORT_EMPTY_VALUE)

    value_str = str(value).strip()
    value_lower = value_str.lower()

    # Check each priority pattern
    for priority, pattern in enumerate(sort_order_list):
        if value_str == pattern or value_str.startswith(pattern):
            return (priority, value_lower)

    # No pattern matches - put after all defined patterns
    return (len(sort_order_list), value_lower)


def get_sort_key_for_column(value, column_name):
    """
    Generate appropriate sort key based on column name and sort order.

    Args:
        value (str): Value to sort
        column_name (str): Name of the column

    Returns:
        tuple: (priority, alphabetic_value) for custom sort, or normalized value
    """
    if column_name in CUSTOM_SORT_MAPPING:
        return get_custom_sort_key(value, CUSTOM_SORT_MAPPING[column_name])
    return str(value).lower() if value else SORT_EMPTY_VALUE


def sort_data(data_rows):
    """
    Sort data rows based on the SORT_COLUMNS configuration.

    Args:
        data_rows (list): List of dictionaries representing CSV rows

    Returns:
        list: Sorted list of rows
    """
    def sort_key(row):
        """Generate sort key for a row based on SORT_COLUMNS configuration."""
        key = []
        for col_name, sort_type in SORT_COLUMNS:
            value = row.get(col_name, '')

            if sort_type == SORT_TYPE_NUMERIC:
                key.append(clean_numeric_value(value))
            elif sort_type == SORT_TYPE_CUSTOM:
                key.append(get_sort_key_for_column(value, col_name))
            else:  # SORT_TYPE_ALPHA
                key.append(str(value).lower() if value else SORT_EMPTY_VALUE)

        return tuple(key)

    return sorted(data_rows, key=sort_key)


def process_csv(input_file, output_file=None):
    """
    Process the CSV file: standardize, extract, merge, and sort data.

    Args:
        input_file (str): Path to input CSV file
        output_file (str): Path to output CSV file (optional)

    Returns:
        Path: Output file path

    Raises:
        FileNotFoundError: If input file doesn't exist
        PermissionError: If unable to write to output file
    """
    input_path = Path(input_file)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    if not input_path.is_file():
        raise ValueError(f"Input path is not a file: {input_path}")

    if output_file is None:
        output_file = input_path.parent / \
            f"{input_path.stem} Cleaned{input_path.suffix}"

    output_path = Path(output_file)

    # Statistics tracking
    stats = {
        'rows_processed': 0,
        'categories_changed': {},
        'wines_updated': 0,
        'mixers_extracted': 0,
        'products_corrected': 0,
        'products_swapped': 0,
    }

    all_rows = []

    # Read and process CSV
    with open(input_path, 'r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)

        for row in reader:
            original_category = row['Category']
            original_product = row['Product']
            original_modifier = row['Modifiers']

            # Step 1: Standardize category
            standardized_category = standardize_category(original_category)
            if original_category != standardized_category:
                if original_category not in stats['categories_changed']:
                    stats['categories_changed'][original_category] = standardized_category

            # Step 2: Apply product-specific corrections (BEFORE swap!)
            # This must happen before swap because corrections are keyed by original product names
            corrected_category = apply_product_category_correction(
                original_product, standardized_category
            )
            if corrected_category != standardized_category:
                stats['products_corrected'] += 1

            # Step 3: Swap product/modifier for promotional items (AFTER corrections!)
            # Now that category is correct, we can safely swap the values
            swapped_product, swapped_modifier = swap_product_modifier(
                original_product, original_modifier
            )
            if swapped_product != original_product:
                stats['products_swapped'] += 1

            # Step 4: Standardize wine products
            cleaned_product, updated_modifier = standardize_wine_product(
                swapped_product, swapped_modifier, corrected_category
            )
            if swapped_product != cleaned_product:
                stats['wines_updated'] += 1

            # Step 5: Extract mixers
            final_modifier, mixer = extract_mixer_from_modifier(
                updated_modifier, corrected_category
            )
            if mixer:
                stats['mixers_extracted'] += 1

            # Step 6: Normalize values globally (BEFORE merging!)
            # This ensures consistent naming so duplicates are properly merged
            normalized_product, normalized_modifier, normalized_mixer = normalize_row_values(
                cleaned_product, final_modifier, mixer
            )

            # Create processed row with normalized values
            processed_row = {
                'Category': corrected_category,
                'Modifiers': normalized_modifier,
                'Product': normalized_product,
                'Mixer': normalized_mixer,
                'Qty': row['Qty'],
                'Item Value': row['Item Value'],
                'Modifier Value': row['Modifier Value'],
                'Gross Product Sales': row['Gross Product Sales'],
                'Cost Price': row['Cost Price'],
                'Gross Profit': row['Gross Profit']
            }

            all_rows.append(processed_row)
            stats['rows_processed'] += 1

    # Step 7: Merge duplicates (now with normalized values)
    merged_rows, duplicates_merged = merge_duplicate_entries(all_rows)

    # Step 8: Sort data
    sorted_rows = sort_data(merged_rows)

    # Write output
    with open(output_path, 'w', encoding='utf-8', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(sorted_rows)

    # Print summary
    print_summary(stats, duplicates_merged, len(sorted_rows), output_path)

    return output_path


def print_normalization_summary():
    """Print normalization rules summary."""
    normalizations_applied = len(
        MIXER_NORMALIZATION) + len(MODIFIER_NORMALIZATION) + len(PRODUCT_NORMALIZATION)
    if normalizations_applied > 0:
        print(f"\n✓ Value normalizations configured:")
        if MIXER_NORMALIZATION:
            print(f"  - Mixer corrections: {len(MIXER_NORMALIZATION)} rules")
            for old_val, new_val in list(MIXER_NORMALIZATION.items())[:3]:
                print(f"    '{old_val}' → '{new_val}'")
            if len(MIXER_NORMALIZATION) > 3:
                print(f"    ... and {len(MIXER_NORMALIZATION) - 3} more")
        if MODIFIER_NORMALIZATION:
            print(
                f"  - Modifier corrections: {len(MODIFIER_NORMALIZATION)} rules")
        if PRODUCT_NORMALIZATION:
            print(
                f"  - Product corrections: {len(PRODUCT_NORMALIZATION)} rules")


def print_sort_configuration():
    """Print sorting configuration."""
    print(f"\n✓ Data sorted by:")
    for col_name, sort_type in SORT_COLUMNS:
        if sort_type == SORT_TYPE_ALPHA:
            sort_desc = "alphabetically"
        elif sort_type == SORT_TYPE_NUMERIC:
            sort_desc = "numerically (ascending)"
        elif sort_type == SORT_TYPE_CUSTOM:
            sort_desc = "custom order"
            if col_name == 'Modifiers' and MODIFIER_SORT_ORDER:
                sort_desc += f" (priority: {' > '.join(MODIFIER_SORT_ORDER[:3])}...)"
            elif col_name == 'Product' and PRODUCT_SORT_ORDER:
                sort_desc += f" (priority: {' > '.join(PRODUCT_SORT_ORDER[:3])}...)"
            elif col_name == 'Mixer' and MIXER_SORT_ORDER:
                sort_desc += f" (priority: {' > '.join(MIXER_SORT_ORDER[:3])}...)"
        else:
            sort_desc = sort_type
        print(f"  - {col_name} ({sort_desc})")


def print_summary(stats, duplicates_merged, final_row_count, output_path):
    """
    Print processing summary.

    Args:
        stats (dict): Processing statistics
        duplicates_merged (int): Number of duplicates merged
        final_row_count (int): Final row count after processing
        output_path (Path): Output file path
    """
    print(f"✓ Processing complete!")
    print(f"  - Rows processed: {stats['rows_processed']}")
    print(f"  - Duplicates merged: {duplicates_merged}")
    print(f"  - Final rows after merging: {final_row_count}")
    print(f"  - Output file: {output_path}")

    if stats['categories_changed']:
        print(f"\n✓ Categories standardized:")
        for old_cat, new_cat in stats['categories_changed'].items():
            print(f"  '{old_cat}' → '{new_cat}'")

    if stats['products_swapped'] > 0:
        print(
            f"\n✓ Product/Modifier swapped: {stats['products_swapped']} entries")
        print(f"  - Products: {', '.join(sorted(PRODUCTS_TO_SWAP))}")
        print(f"  - These were actually modifiers, now corrected")

    if stats['products_corrected'] > 0:
        print(
            f"\n✓ Product category corrections applied: {stats['products_corrected']} products")
        for product_name, correct_cat in PRODUCT_CATEGORY_CORRECTIONS.items():
            print(f"  '{product_name}' → '{correct_cat}'")

    if stats['wines_updated'] > 0:
        print(f"\n✓ Wine products updated: {stats['wines_updated']} products")
        print(f"  - Size information moved from product name to modifiers")

    if stats['mixers_extracted'] > 0:
        print(f"\n✓ Mixers extracted: {stats['mixers_extracted']} products")
        print(f"  - Mixer information separated into 'Mixer' column")

    print_normalization_summary()

    if duplicates_merged > 0:
        print(f"\n✓ Duplicate entries merged: {duplicates_merged} duplicates")
        print(f"  - Entries with same Category/Modifier/Product/Mixer combined")
        print(f"  - Numeric values aggregated")

    print(f"\n✓ Columns reordered: {', '.join(OUTPUT_COLUMNS)}")
    print_sort_configuration()


def main():
    """Main execution function."""
    input_file = "Datasets/Sales by Product and Modifier.csv"

    print("=" * 60)
    print("Sales Data Normalizer for German Kraft Brewing Limited")
    print("=" * 60)
    print(f"\nInput file: {input_file}")

    # Show category mapping rules
    print("\nCategory mapping rules:")
    unique_mappings = {}
    for old_cat, new_cat in CATEGORY_MAPPING.items():
        if old_cat != new_cat:
            if new_cat not in unique_mappings:
                unique_mappings[new_cat] = []
            unique_mappings[new_cat].append(old_cat)

    for new_cat, old_cats in sorted(unique_mappings.items()):
        print(f"  → {new_cat}: {', '.join(sorted(old_cats))}")

    print("\nProcessing...\n")

    try:
        process_csv(input_file)
        print("\n" + "=" * 60)
    except FileNotFoundError as e:
        print(f"\n❌ Error: {e}")
        print("   Please ensure the input file exists.")
    except PermissionError as e:
        print(f"\n❌ Error: {e}")
        print("   Please check file permissions.")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        raise


if __name__ == "__main__":
    main()
