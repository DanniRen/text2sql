import json
import re
from collections import defaultdict


class SQLParser:
    def __init__(self):
        self.operators = {
            'select': 'select',
            'from': 'from',
            'where': 'where',
            'group by': 'groupBy',
            'order by': 'orderBy',
            'having': 'having',
            'limit': 'limit',
            'union': 'union',
            'intersect': 'intersect',
            'except': 'except'
        }

        self.aggregation_ops = {
            'max': 'max',
            'min': 'min',
            'count': 'count',
            'sum': 'sum',
            'avg': 'avg'
        }

        self.condition_ops = {
            '=': '=',
            '>': '>',
            '<': '<',
            '>=': '>=',
            '<=': '<=',
            '!=': '!=',
            'like': 'like',
            'in': 'in',
            'between': 'between',
            'is': 'is'
        }

        self.logical_ops = {
            'and': 'and',
            'or': 'or'
        }

        self.order_ops = {
            'asc': 'asc',
            'desc': 'desc'
        }

    def parse_sql(self, sql):
        # 预处理SQL，移除注释和多余空格
        sql = self._preprocess_sql(sql)

        # 初始化结果结构
        result = {
            'select': None,
            'from': None,
            'where': None,
            'groupBy': None,
            'orderBy': None,
            'having': None,
            'limit': None,
            'union': None,
            'intersect': None,
            'except': None
        }

        # 解析WITH子句（如果存在）
        if sql.strip().lower().startswith('with'):
            sql = self._parse_with_clause(sql)

        # 解析各个子句
        for op in self.operators:
            if op in result:
                pattern = rf'\b{op}\b'
                matches = list(re.finditer(pattern, sql, flags=re.IGNORECASE))

                if matches:
                    # 获取子句位置
                    start_pos = matches[0].start()
                    end_pos = len(sql)

                    # 查找下一个子句的位置
                    for next_op in self.operators:
                        if next_op != op:
                            next_pattern = rf'\b{next_op}\b'
                            next_matches = list(re.finditer(next_pattern, sql[start_pos:], flags=re.IGNORECASE))
                            if next_matches:
                                next_pos = start_pos + next_matches[0].start()
                                if next_pos < end_pos:
                                    end_pos = next_pos

                    # 提取子句内容
                    clause_content = sql[start_pos + len(op):end_pos].strip()

                    # 解析子句
                    if op == 'select':
                        result['select'] = self._parse_select(clause_content)
                    elif op == 'from':
                        result['from'] = self._parse_from(clause_content)
                    elif op == 'where':
                        result['where'] = self._parse_where(clause_content)
                    elif op == 'group by':
                        result['groupBy'] = self._parse_group_by(clause_content)
                    elif op == 'order by':
                        result['orderBy'] = self._parse_order_by(clause_content)
                    elif op == 'having':
                        result['having'] = self._parse_having(clause_content)
                    elif op == 'limit':
                        result['limit'] = self._parse_limit(clause_content)
                    elif op in ['union', 'intersect', 'except']:
                        result[op] = self._parse_set_operation(op, clause_content)

        return result

    def _preprocess_sql(self, sql):
        # 移除注释
        sql = re.sub(r'--.*?\n', '\n', sql)
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)

        # 标准化空格
        sql = re.sub(r'\s+', ' ', sql)

        return sql.strip()

    def _parse_with_clause(self, sql):
        # 简单处理WITH子句，这里只是移除它
        with_pattern = r'\bwith\b.*?\b(?=select)\b'
        match = re.search(with_pattern, sql, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return sql[match.end():].strip()
        return sql

    def _parse_select(self, select_clause):
        # 解析SELECT子句
        is_distinct = False
        if select_clause.lower().startswith('distinct'):
            is_distinct = True
            select_clause = select_clause[len('distinct'):].strip()

        # 分割列
        columns = []
        parts = self._split_by_commas_outside_parentheses(select_clause)

        for part in parts:
            part = part.strip()
            if not part:
                continue

            # 检查是否有聚合函数
            agg_id = 'none'
            for agg_op in self.aggregation_ops:
                if part.lower().startswith(agg_op + '('):
                    agg_id = self.aggregation_ops[agg_op]
                    # 提取列名
                    col_match = re.search(rf'{agg_op}\((.*?)\)', part, flags=re.IGNORECASE)
                    if col_match:
                        col_part = col_match.group(1).strip()
                        col_unit = self._parse_col_unit(col_part)
                        columns.append((agg_id, col_unit))
                        break
            else:
                # 没有聚合函数，直接解析列
                col_unit = self._parse_col_unit(part)
                columns.append(('none', col_unit))

        return (is_distinct, columns)

    def _parse_col_unit(self, col_part):
        # 解析列单元
        # 检查是否是函数调用
        func_match = re.match(r'(\w+)\((.*)\)', col_part)
        if func_match:
            func_name = func_match.group(1)
            args = func_match.group(2)
            # 这里简化处理，将函数调用作为整体
            return (func_name, args)

        # 检查是否有别名
        alias_match = re.match(r'(.+)\s+as\s+(.+)', col_part, flags=re.IGNORECASE)
        if alias_match:
            col_part = alias_match.group(1).strip()

        # 检查是否有表名前缀
        if '.' in col_part:
            table, col = col_part.split('.', 1)
            return (table.strip(), col.strip())

        return (None, col_part.strip())

    def _parse_from(self, from_clause):
        # 解析FROM子句
        table_units = []
        conds = []

        # 简单处理，提取表名和子查询
        # 这里需要更复杂的解析，特别是对于JOIN操作

        # 检查是否有JOIN
        if 'join' in from_clause.lower():
            # 处理JOIN操作
            # 这里简化处理，实际需要更复杂的解析
            parts = re.split(r'\bjoin\b', from_clause, flags=re.IGNORECASE)
            if parts:
                # 第一个表
                first_table = parts[0].strip()
                if first_table.startswith('(') and first_table.endswith(')'):
                    # 子查询
                    subquery = first_table[1:-1].strip()
                    table_units.append(('sql', self.parse_sql(subquery)))
                else:
                    # 表名
                    table_name, alias = self._parse_table_name(first_table)
                    table_units.append(('table_unit', table_name))

                # 处理JOIN条件和后续表
                for i in range(1, len(parts)):
                    join_part = parts[i].strip()
                    # 分割表和条件
                    table_cond_parts = re.split(r'\bon\b', join_part, flags=re.IGNORECASE)
                    if len(table_cond_parts) >= 1:
                        table_part = table_cond_parts[0].strip()
                        if table_part.startswith('(') and table_part.endswith(')'):
                            # 子查询
                            subquery = table_part[1:-1].strip()
                            table_units.append(('sql', self.parse_sql(subquery)))
                        else:
                            # 表名
                            table_name, alias = self._parse_table_name(table_part)
                            table_units.append(('table_unit', table_name))

                        if len(table_cond_parts) > 1:
                            # JOIN条件
                            cond_part = table_cond_parts[1].strip()
                            join_cond = self._parse_condition(cond_part)
                            conds.extend(join_cond)
        else:
            # 没有JOIN，只有一个表或子查询
            if from_clause.startswith('(') and from_clause.endswith(')'):
                # 子查询
                subquery = from_clause[1:-1].strip()
                table_units.append(('sql', self.parse_sql(subquery)))
            else:
                # 表名
                table_name, alias = self._parse_table_name(from_clause)
                table_units.append(('table_unit', table_name))

        return {'table_units': table_units, 'conds': conds}

    def _parse_table_name(self, table_part):
        # 解析表名和别名
        alias_match = re.match(r'(.+)\s+(?:as\s+)?(\w+)', table_part, flags=re.IGNORECASE)
        if alias_match:
            table_name = alias_match.group(1).strip()
            alias = alias_match.group(2).strip()
            return (table_name, alias)
        return (table_part.strip(), None)

    def _parse_where(self, where_clause):
        # 解析WHERE子句
        return self._parse_condition(where_clause)

    def _parse_condition(self, condition_clause):
        # 解析条件
        conditions = []

        # 简单处理，按AND/OR分割
        # 这里需要更复杂的解析，特别是对于嵌套条件和函数调用

        # 分割条件
        parts = self._split_by_logical_ops(condition_clause)

        for part in parts:
            part = part.strip()
            if not part:
                continue

            # 检查是否是逻辑操作符
            if part.lower() in self.logical_ops:
                conditions.append(self.logical_ops[part.lower()])
                continue

            # 解析条件单元
            not_op = False
            if part.lower().startswith('not '):
                not_op = True
                part = part[4:].strip()

            # 查找操作符
            op_found = False
            for op in sorted(self.condition_ops.keys(), key=len, reverse=True):
                if f' {op} ' in part.lower():
                    op_parts = re.split(rf'\s+{re.escape(op)}\s+', part, flags=re.IGNORECASE)
                    if len(op_parts) >= 2:
                        val_unit = self._parse_val_unit(op_parts[0].strip())
                        val1 = self._parse_value(op_parts[1].strip())

                        # 特殊处理BETWEEN
                        if op.lower() == 'between':
                            # 查找AND
                            and_parts = re.split(r'\s+and\s+', op_parts[1], flags=re.IGNORECASE)
                            if len(and_parts) >= 2:
                                val1 = self._parse_value(and_parts[0].strip())
                                val2 = self._parse_value(and_parts[1].strip())
                            else:
                                val2 = None
                        else:
                            val2 = None

                        conditions.append((not_op, self.condition_ops[op.lower()], val_unit, val1, val2))
                        op_found = True
                        break

            if not op_found:
                # 没有找到操作符，可能是函数调用或其他表达式
                conditions.append(part)

        return conditions

    def _parse_val_unit(self, val_part):
        # 解析值单元
        # 检查是否有算术操作符
        for op in ['-', '+', '*', '/']:
            if f' {op} ' in val_part:
                parts = re.split(rf'\s+{re.escape(op)}\s+', val_part)
                if len(parts) >= 2:
                    col_unit1 = self._parse_col_unit(parts[0].strip())
                    col_unit2 = self._parse_col_unit(parts[1].strip())
                    return (op, col_unit1, col_unit2)

        # 没有算术操作符，直接解析列
        return ('none', self._parse_col_unit(val_part), None)

    def _parse_value(self, value_part):
        # 解析值
        # 检查是否是子查询
        if value_part.startswith('(') and value_part.endswith(')'):
            subquery = value_part[1:-1].strip()
            return ('sql', self.parse_sql(subquery))

        # 检查是否是数字
        if re.match(r'^\d+(\.\d+)?$', value_part):
            return ('number', float(value_part))

        # 检查是否是字符串
        if (value_part.startswith('"') and value_part.endswith('"')) or \
                (value_part.startswith("'") and value_part.endswith("'")):
            return ('string', value_part[1:-1])

        # 检查是否是函数调用
        func_match = re.match(r'(\w+)\((.*)\)', value_part)
        if func_match:
            func_name = func_match.group(1)
            args = func_match.group(2)
            return ('function', (func_name, args))

        # 默认作为列名处理
        return ('column', self._parse_col_unit(value_part))

    def _parse_group_by(self, group_by_clause):
        # 解析GROUP BY子句
        col_units = []
        parts = self._split_by_commas_outside_parentheses(group_by_clause)

        for part in parts:
            part = part.strip()
            if not part:
                continue
            col_units.append(self._parse_col_unit(part))

        return col_units

    def _parse_order_by(self, order_by_clause):
        # 解析ORDER BY子句
        val_units = []
        order_type = 'asc'  # 默认升序

        parts = self._split_by_commas_outside_parentheses(order_by_clause)

        for part in parts:
            part = part.strip()
            if not part:
                continue

            # 检查是否有ASC/DESC
            asc_desc_match = re.match(r'(.+)\s+(asc|desc)$', part, flags=re.IGNORECASE)
            if asc_desc_match:
                val_part = asc_desc_match.group(1).strip()
                order_type = asc_desc_match.group(2).lower()
            else:
                val_part = part

            val_units.append(self._parse_val_unit(val_part))

        return (order_type, val_units)

    def _parse_having(self, having_clause):
        # 解析HAVING子句
        return self._parse_condition(having_clause)

    def _parse_limit(self, limit_clause):
        # 解析LIMIT子句
        try:
            return int(limit_clause.strip())
        except ValueError:
            return None

    def _parse_set_operation(self, op, clause):
        # 解析集合操作（UNION/INTERSEPT/EXCEPT）
        # 简化处理，直接递归解析
        return self.parse_sql(clause)

    def _split_by_commas_outside_parentheses(self, text):
        # 分割逗号，但忽略括号内的逗号
        parts = []
        current_part = ""
        paren_count = 0

        for char in text:
            if char == '(':
                paren_count += 1
            elif char == ')':
                paren_count -= 1
            elif char == ',' and paren_count == 0:
                parts.append(current_part)
                current_part = ""
                continue

            current_part += char

        if current_part:
            parts.append(current_part)

        return parts

    def _split_by_logical_ops(self, text):
        # 分割逻辑操作符（AND/OR），但忽略括号内的
        parts = []
        current_part = ""
        paren_count = 0

        i = 0
        while i < len(text):
            char = text[i]

            if char == '(':
                paren_count += 1
            elif char == ')':
                paren_count -= 1
            elif paren_count == 0:
                # 检查是否是AND或OR
                if text[i:i + 5].lower() == ' and ':
                    parts.append(current_part)
                    parts.append('and')
                    current_part = ""
                    i += 5
                    continue
                elif text[i:i + 4].lower() == ' or ':
                    parts.append(current_part)
                    parts.append('or')
                    current_part = ""
                    i += 4
                    continue

            current_part += char
            i += 1

        if current_part:
            parts.append(current_part)

        return parts


# 示例使用
if __name__ == "__main__":
    parser = SQLParser()

    # 示例SQL
    sql = """
          select a.itype, \
                 a.dtstatdate, \
                 datediff(b.dtstatdate, a.dtstatdate) as idaynum, \
                 count(distinct a.vplayerid)          as iusernum
          from (select itype, min(dtstatdate) as dtstatdate, vplayerid \
                from (select '广域战场' as itype, min(dtstatdate) as dtstatdate, vplayerid \
                      from dws_jordass_mode_roundrecord_di \
                      where dtstatdate >= '20240723' \
                        and dtstatdate <= date_add('20240723', 6) \
                        and submodename = '广域战场模式' \
                      group by vplayerid \
                      union all \
                      select '消灭战', min(dtstatdate), vplayerid \
                      from dws_jordass_mode_roundrecord_di \
                      where dtstatdate >= '20230804' \
                        and dtstatdate <= date_add('20230804', 6) \
                        and modename = '组队竞技' \
                        and submodename like '%消灭战模式%' \
                      group by vplayerid) t \
                group by itype, vplayerid) a
                   left join (select '广域战场' as itype, dtstatdate, vplayerid \
                              from dws_jordass_mode_roundrecord_di \
                              where dtstatdate >= '20240723' \
                                and dtstatdate <= date_add('20240723', 13) \
                                and submodename = '广域战场模式' \
                              group by dtstatdate, vplayerid) b on a.itype = b.itype and a.vplayerid = b.vplayerid
          where datediff(b.dtstatdate, a.dtstatdate) between 0 and 7
          group by a.itype, a.dtstatdate, datediff(b.dtstatdate, a.dtstatdate) \
          """

    # 解析SQL
    parsed_sql = parser.parse_sql(sql)

    # 输出结果
    print(json.dumps(parsed_sql, indent=2, ensure_ascii=False))