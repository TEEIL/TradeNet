# The utility functions of Trade Net
#
# Contributors:
# - Mario Wu : wymario@163.com
#
# Create at 2020-05-04
#
import yaml
import pandas as pd
from tqdm import tqdm
from pathlib import Path
from collections import namedtuple

import logging

logger = logging.getLogger("utility")
logger.setLevel(logging.INFO)

# NB. the data directory is temporary as
# the full dataset is saved at an external device, remember to change this.
TEMP_DATA_DIR = Path() / "src"
assert list(TEMP_DATA_DIR.iterdir()), "mis-configured: empty directory."

PROD_CODES = pd.read_csv(str(TEMP_DATA_DIR / "product_codes_HS92_V202001.csv"))
COUNTRY_CODES = pd.read_csv(str(TEMP_DATA_DIR / "country_codes_V202001.csv"), encoding="gbk")

# NB. save memory and loading time by globalize the data facet
global global_named_facet


def load_code_from_yaml(file_name):
    load_file = TEMP_DATA_DIR / file_name
    assert load_file.exists(), "mis-configured: %s not existed" % str(load_file)

    with load_file.open("r") as file:
        return yaml.safe_load(file.read())


def make_product_code(code: int):
    return "{0:06d}".format(int(float(code)))


def query_country(country_code):
    """query country code (numeric or ISO3 code) from the country codes list."""
    if isinstance(country_code, int):
        try:
            index = COUNTRY_CODES.country_code.to_list().index(int(country_code))
        except IndexError:
            logger.error("invalid country code: %d" % country_code)
            return
    elif isinstance(country_code, str):
        query = COUNTRY_CODES[COUNTRY_CODES.iso_3digit_alpha.isin([country_code])]
        if query.empty:
            return
        index = query.index[0]

    row = COUNTRY_CODES.loc[index, ].to_dict()
    Country = namedtuple("country", ["code", "abbr", "name", "iso2", "iso3"])
    # create a namedtuple for easier usage:
    return Country._make(list(row.values()))


def query_product(product_code: int):
    """query product code (digit) from the product codes list."""
    try:
        index = PROD_CODES.code.to_list().index(make_product_code(int(product_code)))
    except IndexError:
        logger.error("invalid product code: %s" % product_code)
        return

    return PROD_CODES.loc[index, ].to_dict()


def load_data_facet(year):
    for file in TEMP_DATA_DIR.iterdir():
        if ("Y%d" % year) in file.name:
            logger.info("loading: %s" % str(file.resolve()))
            return pd.read_csv(str(file))

    assert 0, "invalid year query for the current dataset"


def _create_bilateral_links(year, ci: str or list, cj: str or list, code: str or list,
                            compress_country=False, compress_product=True, is_panel=False):
    """create links between two countries.
    :param ci: the source country i's ISO3 digit code
    :param cj: the target country j's ISO3 digit code
    :param code: specify a product code to extract links
    :param compress_country: boolean, compress values by countries from both sides
    :param compress_product: boolean, compress values by products
    :param is_panel: boolean, output fully balanced panel data

    ``` Example data:
        t    i   j     k       v       q
    0  2000  4  12   90920   2.823   6.800
    1  2000  4  12   90930  11.216  11.550
    2  2000  4  12   90940   7.981  19.140
    ```
    where t(year), i(source country), j(target country), k(product code),
    v(value: in current 1,000 dollars), q(quantity: metric tons)
    """
    def _list_instance(item):
        if isinstance(item, list):
            return item
        else:
            return [item]

    global_named_facet = globals().get("named_facet", {})
    if global_named_facet and global_named_facet.get("year") == year:
        facet = global_named_facet[year]
    else:
        facet = load_data_facet(year)
        named_facet = {year: facet}  # named facet by labelling the year

    # convert country codes (str) into numeric codes
    if code:
        code = _list_instance(code)

    if ci:
        ci = _list_instance(ci)
        ci = [query_country(c).code for c in ci if query_country(c)]
        facet = facet[facet.i.isin(ci)]

    if cj:
        cj = _list_instance(cj)
        cj = [query_country(c).code for c in cj if query_country(c)]
        facet = facet[facet.j.isin(cj)]
    # grouping by countries and product codes
    result = pd.DataFrame()
    for country_code, index in facet.groupby(["i"]).groups.items():
        subset = facet.loc[index, ]
        subset = subset[subset.k.isin(code)] if code else subset

        if compress_product:  # True by default
            subset = subset.groupby(["j", "k"]).sum().reset_index()

        # NB. `.iso3` is overwritten in `query_country()` function.
        subset.i = [query_country(each).iso3 for each in subset.i]
        subset.j = [query_country(each).iso3 for each in subset.j]
        # append tables continuously.
        result = pd.concat([result, subset.copy(deep=True)])

    if compress_country:  # compress the data frame by country pairs.
        result = result[["i", "j", "v"]].groupby(["i", "j"]).sum().reset_index()

    if is_panel:  # fill up countries for imbalanced relations
        all_countries = set(result.i.values) | set(result.j.values)
        table = pd.DataFrame()
        for c in all_countries:
            row = {each: 0 for each in all_countries if each != c}
            c_row = result[result.i == c]
            row.update(c_row.set_index(c_row.j).v.to_dict())

            c_row = pd.DataFrame({c: row}).T.reset_index()
            c_row = c_row.melt(id_vars=["index"], value_vars=list(row.keys()))
            table = pd.concat([table, c_row], axis=0)

        table.columns = ["i", "j", "v"]
        result = table.copy(deep=True)

    return result


def fetch_links_by_years(years: list, ci=[], cj=[], code=[], **params):
    """create a panel including ordered by years,
    allow params according to method: create_bilateral_links"""
    result = pd.DataFrame()
    for year in tqdm(years):
        created = _create_bilateral_links(year, ci, cj, code, **params)
        # append a new column including year information
        created["year"] = year
        result = pd.concat([result, created], axis=0)

    return result


def _load_hs_mapping_by_years(origin_year: int, target_year: int):
    filename = "hs%s_hs%s.xls" % (origin_year, target_year)
    file = TEMP_DATA_DIR / filename
    assert file.exists(), "HS mapping from %d to %d not found" % (origin_year, target_year)

    mapping = pd.read_excel(str(file), sheet_name="Conversion Tables", dtype="object")  # caution `sheet name`
    mapping.columns = ["origin", "target"]
    mapping = mapping.dropna()
    return mapping


def query_hs_code_by_years(origin_year, target_year, codes: int or list):
    """return a converted HS code after comparing between the origin and target year"""
    mapping = _load_hs_mapping_by_years(origin_year, target_year)
    if isinstance(codes, int):
        codes = [codes]

    out = []
    for code in codes:
        coded = make_product_code(code)
        queried = mapping[mapping.origin == coded]
        if queried.empty:
            out.append(coded)
        else:
            out.append(queried.target.values[0])

    return out
