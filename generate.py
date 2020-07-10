#!/usr/bin/env python
#
# The generate function of Trade Net
#
# Contributors:
# - Mario Wu : wymario@163.com
#
# Create at 2020-05-04
#
import yaml
import click
from pathlib import Path
from utils import create_bilateral_links


@click.group(invoke_without_command=True)
@click.option("-c", "--code-book", default=Path("src") / "code_book.yaml",
              help="codebook for extract data by HS6 digits")
@click.pass_context
def main(ctx, code_book):
    """Run generate to fetch dataset from the entire database"""
    # code-book: specify the product codes of interests.
    if not code_book.exists():
        ctx.utils.echo("Invalid dir path of codebook is given, please check around it.")

    ctx.obj = {}
    # load codebook for product code queries.
    try:
        with code_book.open() as f:
            ctx.obj["codebook"] = yaml.safe_load(f)
    except yaml.parser.ParserError as err:
        print("ParseError: %s" % err)


@main.command()
@click.option("-i", "--source", default="ALL", help="specify the source country, default: ALL")
@click.option("-j", "--target", default="ALL", help="specify the target country, default: ALL")
@click.option("-o", "--output-dir", default=Path("") / "_output",
              help="output dir path, default: /_output/")
@click.pass_context
def fetch(ctx, source, target, output_dir):
    """Run fetch command to get a data facet output"""
    pass


if __name__ == "__main__":  # pragma not cover
    main()
