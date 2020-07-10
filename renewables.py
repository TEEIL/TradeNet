# This is a demo script for renewable energy trading networks
#
from pathlib import Path
from utils import query_hs_code_by_years, make_product_code, load_code_from_yaml, fetch_links_by_years

# save dir path
SAVE_DIR = Path("save")
# pasted from the ape_dataset_v4
COUNTRIES = ['SGP', 'PHL', 'MYS', 'NPL', 'IRN', 'MMR', 'VNM', 'MHL', 'UZB',
             'FJI', 'NZL', 'AUS', 'IND', 'HKG', 'WSM', 'BRN', 'GEO', 'NRU',
             'MAC', 'PAK', 'LAO', 'KAZ', 'BGD', 'THA', 'MNG', 'TUR', 'CHN',
             'AFG', 'BTN', 'IDN', 'MDV', 'NIU', 'KIR', 'TJK', 'KHM', 'KGZ',
             'RUS', 'KOR', 'TUV', 'JPN', 'COK', 'TON', 'AZE', 'FSM', 'MNP',
             'VUT', 'ARM', 'SLB', 'PNG', 'LKA', 'Other', 'GUM', 'TKM', 'NCL',
             'PLW', 'ASM', 'TLS', 'PRK']

if __name__ == "__main__":
    # TODO: make the following codes as template.
    codes = load_code_from_yaml("renew_energy_codes_from_reference.yaml")
    codes = [make_product_code(each) for each in list(codes["wind"].keys())] + \
            [make_product_code(each) for each in list(codes["solar"].keys())]
    query_codes = query_hs_code_by_years(2007, 1992, codes)

    # create data sheet from multiple years
    result = fetch_links_by_years(range(2000, 2019, 1), ci=COUNTRIES, cj=COUNTRIES, code=query_codes,
                                  compress_country=True, is_panel=True)

    result.to_csv(str(SAVE_DIR / "bi_trade_flows.csv"), index=False)
