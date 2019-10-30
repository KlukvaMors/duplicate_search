from dds import Duplicate
import config
import json

ID = config.get('db', 'id_column')
COLUMNS = ([ID] + [s.strip() for s in config.get('dds', 'exact_match_columns').split(',')] +
           [s.strip() for s in config.get('dds', 'tolerance_match_columns').split(',')])


def header() -> str:
    result = ''
    for col in COLUMNS:
        result += f'\t\t<th>{col}</th>\n'
    return f'\t<tr>\n{result}\t</tr>\n'


def row(data: dict, top: bool) -> str:
    result = ''
    for col in COLUMNS:
        result += f'\t\t<td>{data[col]}</td>\n'
    return f'\t<tr class={"top" if top else "bottom"}>\n{result}\t</tr>\n'


def table() -> str:
    result = ''
    for i, duplicate in enumerate(Duplicate.select().execute()):
        data = json.loads(duplicate.obj1_json)
        result += row(data, bool(i % 2))
        data = json.loads(duplicate.obj2_json)
        result += row(data, bool(i % 2))
    return f'<table>\n{result}\n</table>'


def create_report(template_file='report_template.html', report_file='report.html'):
    with open(template_file, encoding='utf8') as ftemplate:
        template = ftemplate.read()
        with open(report_file, 'w', encoding='utf8') as freport:
            freport.write(template % table())


if __name__=='__main__':
    create_report()
    print("Report ready!")