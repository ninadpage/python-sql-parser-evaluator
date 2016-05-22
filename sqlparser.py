from collections import namedtuple, OrderedDict

from pyparsing import CaselessKeyword, Combine, Group, Literal, Optional, Word, alphanums, alphas, delimitedList, \
    nums, oneOf, opAssoc, quotedString, replaceWith, infixNotation


__author__ = "ninadpage"


LHS = 0
OP = 1
RHS = 2

AND_KEYWORD = "AND"
OR_KEYWORD = "OR"

WhereCondition = namedtuple('WhereCondition', ['lhs', 'op', 'rhs'])


def parse_where_condition(toks):
    return WhereCondition(lhs=toks[0], op=toks[1], rhs=toks[2])


lpar = Literal("(").suppress()
rpar = Literal(")").suppress()
backtick = Literal("`").suppress()
singlequote = Literal("'").suppress()
doublequote = Literal("\"").suppress()

identifier = Combine(Word(alphas + "_", alphanums + "_$"))
columnName = identifier | (backtick + identifier + backtick)
tableName = identifier | (backtick + identifier + backtick)

arithSign = Word("+-", exact=1)
intNum = Combine(Optional(arithSign) + Word(nums)).setParseAction(lambda toks: int(toks[0]))
realNum = Combine(Optional(arithSign) + Word(nums) + "." + Word(nums)).setParseAction(lambda toks: float(toks[0]))
# TODO Support scientific notation with a mantissa? e.g. 1e10
columnRval = intNum | realNum | quotedString.setParseAction(lambda toks: toks[0][1:-1])  # Removes quotes

eq_ = Literal("=").setParseAction(replaceWith('eq_'))
neq_ = oneOf("!= <>").setParseAction(replaceWith('neq_'))
gt_ = oneOf(">").setParseAction(replaceWith('gt_'))
ge_ = oneOf(">=").setParseAction(replaceWith('ge_'))
lt_ = oneOf("<").setParseAction(replaceWith('lt_'))
le_ = oneOf("<=").setParseAction(replaceWith('le_'))
in_ = CaselessKeyword("in").setParseAction(replaceWith('in_'))
nin_ = CaselessKeyword("not in").setParseAction(replaceWith('nin_'))
# TODO Add support for LIKE

and_ = CaselessKeyword(AND_KEYWORD)
or_ = CaselessKeyword(OR_KEYWORD)
# TODO Add support for NOT

whereCondition = (
    columnName + (eq_ | neq_ | gt_ | ge_ | lt_ | le_) + columnRval |
    columnName + (in_ | nin_) + lpar + Group(delimitedList(columnRval)) + rpar
).setParseAction(parse_where_condition)
# TODO Add support for arithmetic expressions in where conditions
# TODO Add nested query support for IN/NOT IN

# whereExpression = Forward()
# whereExpression << Group(
#     whereCondition + ZeroOrMore((and_ | or_) + whereExpression) |
#     lpar + whereExpression + rpar
# ).setParseAction(evaluate_expression)

whereClause = infixNotation(whereCondition,
                            [
                                (and_, 2, opAssoc.LEFT),
                                (or_, 2, opAssoc.LEFT),
                            ]
                            )

selectKeyword = CaselessKeyword("select")
fromKeyword = CaselessKeyword("from")
columnList = delimitedList(columnName)
whereKeyword = CaselessKeyword("where")

selectStatement = (
    selectKeyword +
    Group(Literal("*") | columnList).setResultsName('columns') +
    fromKeyword +
    tableName.setResultsName('table') +
    Optional(whereKeyword + whereClause.setResultsName('whereexpr'))
)


def evaluate_where_condition(wherecondition, row):
    if wherecondition.op == 'eq_':
        return row[wherecondition.lhs] == wherecondition.rhs
    elif wherecondition.op == 'neq_':
        return row[wherecondition.lhs] != wherecondition.rhs
    elif wherecondition.op == 'gt_':
        return row[wherecondition.lhs] > wherecondition.rhs
    elif wherecondition.op == 'ge_':
        return row[wherecondition.lhs] >= wherecondition.rhs
    elif wherecondition.op == 'lt_':
        return row[wherecondition.lhs] < wherecondition.rhs
    elif wherecondition.op == 'le_':
        return row[wherecondition.lhs] <= wherecondition.rhs
    elif wherecondition.op == 'in_':
        return row[wherecondition.lhs] in wherecondition.rhs
    elif wherecondition.op == 'nin_':
        return row[wherecondition.lhs] not in wherecondition.rhs


def evaluate_where_expression(tree, row):
    if isinstance(tree, WhereCondition):
        return evaluate_where_condition(tree, row)
    if len(tree) > 1:
        if tree[OP] == AND_KEYWORD:
            return evaluate_where_expression(tree[LHS], row) and evaluate_where_expression(tree[RHS:], row)
        if tree[OP] == OR_KEYWORD:
            return evaluate_where_expression(tree[LHS], row) or evaluate_where_expression(tree[RHS:], row)
    else:
        return evaluate_where_expression(tree[0], row)


def get_projection(row, columns):
    """
    Extracts & returns fields specified in `columns` from `row`. Returns all fields if columns is ['*'].

    :param row: A dict representing a row from database
    :type row: dict
    :param columns: A list of columns to project
    :type columns: list<pyparsing.ParseResults>
    :return: Projection of `row`
    :rtype: OrderedDict
    """
    if columns[0] == '*':
        return OrderedDict(row)
    else:
        result = OrderedDict()
        for column in columns:
            result[column] = row[column]
        return result


def evaluate_select_statement(db, select_statement):
    parsed_statement = selectStatement.parseString(select_statement)
    table = parsed_statement['table']

    selected_rows = []
    for row in db[table]:
        if 'whereexpr' in parsed_statement:
            match = evaluate_where_expression(parsed_statement['whereexpr'], row)
            if match:
                selected_rows.append(row)
        else:
            # Missing WHERE clause => no filtering
            selected_rows.append(row)

    result = []
    columns = parsed_statement['columns']
    for row in selected_rows:
        result.append(get_projection(row, columns))

    return result
