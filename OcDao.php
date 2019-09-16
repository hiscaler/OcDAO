<?php

/**
 * OpenCart Database Access Objects
 *
 * @author hiscaler <hiscaler@gmail.com>
 */
class OcDao
{

    /**
     * @var $db DB
     */
    private $db;

    /**
     * @var bool Active debug mode, if set to true, will print SQL string and call back traces.
     */
    private $debug = false;

    private $table;
    private $select = '*';
    private $selectSet = [];
    private $where;
    private $offset = 0;
    private $limit = null;
    private $orderBy;
    private $leftJoin;
    private $tableQuoteCharacter = "`";
    private $columnQuoteCharacter = '`';
    private $indexBy;
    private $groupBy;
    private $having;
    private $conditions = [];

    /**
     * @var bool 是否为手工书写的 SQL
     */
    private $isCommand = false;

    /**
     * @var string SQL
     */
    private $sql;

    public function __construct($registry, $debug = false)
    {
        $this->db = $registry->get('db');
        $this->debug = $debug;
    }

    private function quoteSimpleTableName($name)
    {
        if (is_string($this->tableQuoteCharacter)) {
            $startingCharacter = $endingCharacter = $this->tableQuoteCharacter;
        } else {
            list($startingCharacter, $endingCharacter) = $this->tableQuoteCharacter;
        }

        return strpos($name, $startingCharacter) !== false ? $name : $startingCharacter . $name . $endingCharacter;
    }

    /**
     * Quote table name
     *
     * @param $name
     * @return string
     */
    private function quoteTableName($name)
    {
        if (strpos($name, '(') !== false || strpos($name, '{{') !== false) {
            return $name;
        }
        if (strpos($name, '.') === false) {
            return $this->quoteSimpleTableName($name);
        }
        $parts = explode('.', $name);
        foreach ($parts as $i => $part) {
            $parts[$i] = $this->quoteSimpleTableName($part);
        }

        return implode('.', $parts);
    }

    private function quoteSimpleColumnName($name)
    {
        if (is_string($this->tableQuoteCharacter)) {
            $startingCharacter = $endingCharacter = $this->columnQuoteCharacter;
        } else {
            list($startingCharacter, $endingCharacter) = $this->columnQuoteCharacter;
        }

        return $name === '*' || strpos($name, $startingCharacter) !== false ? $name : $startingCharacter . $name . $endingCharacter;
    }

    /**
     * Quote column name
     *
     * @param $name
     * @return bool|string
     */
    private function quoteColumnName($name)
    {
        if (strpos($name, '(') !== false || strpos($name, '[[') !== false) {
            return $name;
        }
        if (($pos = strrpos($name, '.')) !== false) {
            $prefix = $this->quoteTableName(substr($name, 0, $pos)) . '.';
            $name = substr($name, $pos + 1);
            if (($pos = stripos($name, ' AS ')) !== false) {
                $prefix = $prefix . $this->quoteTableName(substr($name, 0, $pos)) . ' AS ';
                $name = substr($name, $pos + 4);
            }
        } elseif (($pos = stripos($name, ' AS ')) !== false) {
            $prefix = $this->quoteTableName(substr($name, 0, $pos)) . ' AS ';
            $name = substr($name, $pos + 4);
        } else {
            $prefix = '';
        }
        if (strpos($name, '{{') !== false) {
            return $name;
        }

        return $prefix . $this->quoteSimpleColumnName($name);
    }

    private function quoteValue($value)
    {
        if (!is_string($value)) {
            return $value;
        }

        return "'" . addcslashes(str_replace("'", "''", $value), "\000\n\r\\\032") . "'";
    }

    /**
     * Collection SQL string, Not complete SQL statement
     *
     * @return string
     */
    private function collectionSql()
    {
        $sql = '';
        if ($this->leftJoin) {
            $sql .= " $this->leftJoin";
        }
        if ($this->where = $this->parseConditions($this->conditions)) {
            $sql .= " WHERE {$this->where}";
        }
        if ($this->groupBy) {
            $sql .= " GROUP BY {$this->groupBy}";
            if ($this->having) {
                $sql .= " HAVING {$this->having}";
            }
        }
        if ($this->orderBy) {
            $sql .= " ORDER BY {$this->orderBy}";
        }
        if ($this->limit) {
            $sql .= " LIMIT $this->offset, $this->limit";
        }

        return $sql;
    }

    /**
     * Quote SQL string
     *
     * @param $sql
     * @return string|string[]|null
     */
    private function quoteSql($sql)
    {
        $sql = preg_replace_callback(
            '/(\\{\\{(%?[\w\-\. ]+%?)\\}\\}|\\[\\[([\w\-\. ]+)\\]\\])/',
            function ($matches) {
                if (isset($matches[3])) {
                    return $this->quoteColumnName($matches[3]);
                }

                return str_replace('%', DB_PREFIX, $this->quoteTableName($matches[2]));
            },
            $sql
        );

        return $sql;
    }

    /**
     * Reset all value
     *
     * @return $this
     */
    public function reset()
    {
        $this->table = null;
        $this->select = '*';
        $this->selectSet = [];
        $this->where = null;
        $this->offset = 0;
        $this->limit = null;
        $this->orderBy = null;
        $this->leftJoin = null;
        $this->indexBy = null;
        $this->groupBy = null;
        $this->having = null;
        $this->sql = null;
        $this->isCommand = false;
        $this->conditions = [];

        return $this;
    }

    /**
     * Insert new record
     *
     * @param $table
     * @param $columns
     * @return $this
     */
    public function insert($table, $columns)
    {
        $fields = [];
        $values = [];
        foreach ($columns as $column => $value) {
            $fields[] = $this->quoteColumnName($column);
            $values[] = $this->quoteValue($value);
        }
        $this->sql = sprintf('INSERT INTO %s (%s) VALUES (%s)',
            $this->quoteTableName($table),
            implode(', ', $fields),
            implode(', ', $values)
        );

        return $this;
    }

    /**
     * Batch insert new records
     *
     * @param $table
     * @param $columns
     * @param $rows
     * @return $this
     */
    public function batchInsert($table, $columns, $rows)
    {
        $fields = [];
        foreach ($columns as $name) {
            $fields[] = $this->quoteColumnName($name);
        }
        $values = [];
        foreach ($rows as $row) {
            foreach ($row as $k => $v) {
                $row[$k] = $this->quoteValue($v);
            }
            $values[] = '(' . implode(', ', $row) . ')';
        }
        $this->sql = sprintf('INSERT INTO %s (%s) VALUES %s',
            $this->quoteTableName($table),
            implode(', ', $fields),
            implode(', ', $values)
        );

        return $this;
    }

    /**
     * Add condition
     *
     * @param $operation
     * @param $condition
     * @param array $params
     * @throws Exception
     */
    private function addCondition($operation, $condition, $params = [])
    {
        var_dump($operation);
        if (!in_array(strtolower($operation), ['AND', 'OR'])) {
            throw new Exception("$operation is not a valid operation.");
        }
        $this->conditions[] = [$operation, $condition, $params];
    }

    /**
     * 条件解析
     *
     * ["id" => 1] : id = 1
     * ["id" => [1, 2]] : id IN (1, 2)
     * ["id" => []] : 0 = 1
     * ["remark" => NULL] : remark IS NULL
     * ["AND", ["id" => 1], ["LIKE", "username", "hiscaler"]] : id = 1 AND username LIKE "%hiscaler%"
     *
     * @param $conditions
     * @return string
     */
    private function parseConditions($conditions)
    {
        $where = '';
        $fnFixValues = function ($values, $quote = true) {
            $vs = [];
            if (is_array($values)) {
                foreach ($values as $value) {
                    if ($quote) {
                        $value = is_numeric($value) ? $value : $this->quoteValue($value);
                    }

                    if (!in_array($value, $vs)) {
                        $vs[] = $value;
                    }
                }
            } else {
                if ($quote) {
                    $vs[] = is_numeric($values) ? $values : $this->quoteValue($values);
                } else {
                    $vs[] = $values;
                }
            }

            return $vs;
        };
        foreach ($conditions as $i => $data) {
            $operation = strtoupper($data[0]);
            if ($i > 0) {
                $where .= " $operation ";
            }
            $condition = $data[1];
            $params = $data[2];
            if (is_string($condition)) {
                $where .= $condition;
                if ($params && is_array($params)) {
                    $where = strtr($where, $params);
                }
            } elseif (is_array($condition)) {
                if (!$condition) {
                    continue;
                }
                $ws = [];
                $indexed = true;
                foreach ($condition as $k => $v) {
                    if (is_string($k)) {
                        $indexed = false;
                        break;
                    }
                }
                if ($indexed) {
                    $columnName = $this->quoteColumnName($condition[1]);

                    $innerOperation = isset($condition[3]) ? strtolower(trim($condition[3])) : 'AND';
                    if (!in_array($innerOperation, ['AND', "OR"])) {
                        $innerOperation = "AND";
                    }
                    $sqlKeyword = strtoupper(trim($condition[0]));
                    switch ($sqlKeyword) {
                        case "IN":
                            $vs = $fnFixValues($condition[2], true);
                            $s = isset($vs[1]) ? "$columnName IN (" . implode(", ", $vs) . ")" : "$columnName = {$vs[0]}";
                            break;

                        case "NOT IN":
                            $vs = $fnFixValues($condition[2], true);
                            $s = isset($vs[1]) ? "$columnName NOT IN (" . implode(", ", $vs) . ")" : "$columnName <> {$vs[0]}";
                            break;

                        case "LIKE":
                        case "NOT LIKE":
                            $vs = $fnFixValues($condition[2], false);
                            if (isset($vs[1])) {
                                $likes = [];
                                foreach ($vs as $v) {
                                    $likes[] = "$columnName $sqlKeyword '%{$v}%'";
                                }
                                $s = implode(" $innerOperation ", $likes);
                            } else {
                                $s = "$columnName $sqlKeyword '%{$vs[0]}%'";
                            }
                            break;

                        default:
                            $s = '';
                            break;
                    }
                    $s && $ws[] = $s;
                } else {
                    foreach ($condition as $key => $value) {
                        $columnName = $this->quoteColumnName($key);
                        if (is_array($value)) {
                            if ($value) {
                                $vs = $fnFixValues($value);
                                $s = isset($vs[1]) ? "$columnName IN (" . implode(", ", $vs) . ")" : "$columnName = {$vs[0]}";
                            } else {
                                $s = "0 = 1";
                            }
                        } elseif (is_string($value) || is_numeric($value)) {
                            $s = "$columnName = " . (is_numeric($value) ? $value : $this->quoteValue($value));
                        } elseif (is_null($value)) {
                            $s = "$columnName IS NULL";
                        } else {
                            $s = "0 = 1";
                        }
                        $s && $ws[] = $s;
                    }
                }
                $ws && $where .= "(" . implode(" AND ", $ws) . ")";
            }
        }

        return $where;
    }

    /**
     * Update record
     *
     * @param $table
     * @param $columns
     * @param string $condition
     * @param array $params
     * @return $this
     * @throws Exception
     */
    public function update($table, $columns, $condition = '', $params = [])
    {
        if ($columns) {
            $sets = [];
            foreach ($columns as $k => $v) {
                $sets[] = self::quoteColumnName($k) . ' = ' . $this->quoteValue($v);
            }
            $sql = "UPDATE " . $this->quoteTableName($table) . " SET " . implode(", ", $sets);
            $this->where($condition, $params);
            if ($where = $this->parseConditions($this->conditions)) {
                $sql .= " WHERE $where";
            }
            $this->sql = $sql;

            return $this;
        } else {
            throw new Exception("Update column error.");
        }
    }

    /**
     * Delete record
     *
     * @param $table
     * @param string $condition
     * @param array $params
     * @return $this
     * @throws Exception
     */
    public function delete($table, $condition = '', $params = [])
    {
        $sql = "DELETE FROM " . $this->quoteTableName($table);
        $this->where($condition, $params);
        $where = $this->parseConditions($this->conditions);
        $where && $sql .= " WHERE $where";
        $this->sql = $sql;

        return $this;
    }

    private function _select($select, $append = false)
    {
        if ($select && is_string($select)) {
            $select = explode(",", $select);
        } elseif (!is_array($select)) {
            $select = [];
        }
        if ($select) {
            $select = array_filter(array_unique($select));
            $ss = [];
            foreach ($select as $v) {
                $v = trim($v);
                if (in_array($v, $this->selectSet)) {
                    continue;
                } else {
                    $this->selectSet[] = $v;
                    $ss[] = $v != '*' ? $this->quoteColumnName($v) : $v;
                }
            }
            if ($ss) {
                if ($append) {
                    $select = implode(", ", $ss);
                    $originalSelect = $this->select;
                    if ($originalSelect) {
                        $this->select = "$originalSelect, $select";
                    } else {
                        $this->select = $select;
                    }
                } else {
                    $this->select = implode(", ", $ss);
                }
            }
        }
    }

    /**
     * Process `SELECT` sql names
     *
     * @param $select
     * @return $this
     */
    public function select($select)
    {
        $this->_select($select, false);

        return $this;
    }

    /**
     * Add `SELECT` SQL names
     *
     * @param $select
     * @return $this
     */
    public function addSelect($select)
    {
        $this->_select($select, true);

        return $this;
    }

    /**
     * Process `FROM` table
     *
     * @param $table
     * @return $this
     */
    public function from($table)
    {
        $this->table = $this->quoteTableName($table);

        return $this;
    }

    /**
     * Where
     *
     * @param $condition
     * @param $params
     * @return $this
     * @throws Exception
     */
    public function where($condition, $params = [])
    {
        $this->conditions = [
            ['AND', $condition, $params]
        ];

        return $this;
    }

    /**
     * AND WHERE
     *
     * @param $condition
     * @param array $params
     * @return $this
     * @throws Exception
     */
    public function andWhere($condition, $params = [])
    {
        $this->addCondition('AND', $condition, $params);

        return $this;
    }

    /**
     * OR WHERE
     *
     * @param $condition
     * @param array $params
     * @return $this
     * @throws Exception
     */
    public function orWhere($condition, $params = [])
    {
        $this->addCondition('OR', $condition, $params);

        return $this;
    }

    /**
     * Process `ORDER BY` SQL
     *
     * @param $orders
     * @return $this
     */
    public function orderBy($orders)
    {
        $s = [];
        if ($orders) {
            if (is_array($orders)) {
                foreach ($orders as $k => $v) {
                    $s[] = $this->quoteColumnName($k) . ($v == SORT_DESC ? ' DESC' : ' ASC');
                }
            }
        }

        $this->orderBy = $s ? implode(', ', $s) : '';

        return $this;
    }

    /**
     * Group By
     *
     * @param $name
     * @return $this
     */
    public function groupBy($name)
    {
        $this->groupBy = $this->quoteColumnName($name);

        return $this;
    }

    /**
     * Having
     *
     * @param $condition
     * @param array $params
     * @return $this
     * @throws Exception
     */
    public function having($condition, $params = [])
    {
        $this->having = $this->parseConditions([['AND', $condition, $params]]);

        return $this;
    }

    /**
     * Process `OFFSET` SQL
     *
     * @param $n
     * @return $this
     */
    public function offset($n)
    {
        $n = (int) $n;
        $this->offset = $n < 0 ? 0 : $n;

        return $this;
    }

    /**
     * Process `LIMIT` SQL
     *
     * @param $n
     * @return $this
     */
    public function limit($n)
    {
        $n = (int) $n;
        $this->limit = $n < 0 ? 0 : $n;

        return $this;
    }

    /**
     * Add index name
     *
     * @param $name
     * @return $this
     */
    public function indexBy($name)
    {
        $this->indexBy = $name;
        $this->addSelect($name);

        return $this;
    }

    /**
     * Left Join
     *
     * @param $table
     * @param $condition
     * @param $params
     * @return $this
     * @throws Exception
     */
    public function leftJoin($table, $condition = null, $params = [])
    {
        $sql = "LEFT JOIN " . $this->quoteTableName($table);
        if (is_array($condition)) {
            $list = [];
            foreach ($condition as $key => $value) {
                $list[] = $this->quoteColumnName($key) . ' = ' . $this->quoteValue($value);
            }
            $condition = implode(' AND ', $list);
        }
        if ($where = $this->parseConditions([['AND', $condition, $params]])) {
            $sql .= " ON $where";
        }
        if ($this->leftJoin) {
            $sql = "{$this->leftJoin} $sql";
        }
        $this->leftJoin = $sql;

        return $this;
    }

    /**
     * 返回一条记录
     *
     * @return mixed
     * @throws Exception
     */
    public function one()
    {
        if (!$this->isCommand) {
            $this->limit(1);
            $sql = "SELECT {$this->select} FROM {$this->table}";
            if ($sql2 = $this->collectionSql()) {
                $sql .= " $sql2";
            }
            $this->sql = $sql;
        }

        $q = $this->_execute();

        return $q === false ? false : $q->row;
    }

    /**
     * 返回多条记录
     *
     * @return array
     */
    public function all()
    {
        if (!$this->isCommand) {
            $sql = "SELECT {$this->select} FROM {$this->table}";
            if ($sql2 = $this->collectionSql()) {
                $sql .= " $sql2";
            }

            $this->sql = $sql;
        }
        $q = $this->_execute();
        $rawRows = $q === false ? [] : $q->rows;
        if ($rawRows && $this->indexBy) {
            $indexName = $this->indexBy;
            if (($pos = strrpos($indexName, ".")) !== false) {
                $indexName = substr($indexName, $pos);
            }
            $indexName = str_replace("`", '', $indexName);
            $rows = [];
            foreach ($rawRows as $row) {
                $rows[$row[$indexName]] = $row;
            }
        } else {
            $rows = $rawRows;
        }

        return $rows;
    }

    /**
     * @return bool If not found, will return false
     * @throws Exception
     */
    public function scalar()
    {
        $res = false;
        $data = $this->one();
        if ($data !== false) {
            foreach ($data as $k => $v) {
                if ($k == 0) {
                    $res = $v;
                    break;
                }
            }
        }

        return $res;
    }

    public function column()
    {
        $res = [];
        $valueKey = null;
        foreach ($this->all() as $k => $row) {
            foreach ($row as $kk => $vv) {
                if ($this->indexBy) {
                    if ($kk == $this->indexBy) {
                        if ($valueKey === null) {
                            foreach ($this->selectSet as $s) {
                                if ($s != $this->indexBy) {
                                    $valueKey = $s;
                                    break;
                                }
                            }
                        }
                        $res[$k] = $row[$valueKey];
                        break;
                    }
                } else {
                    if ($kk == 0) {
                        $res[] = $vv;
                        break;
                    }
                }
            }
        }

        return $res;
    }

    /**
     * @return boolean
     * @throws Exception
     */
    public function exist()
    {
        return $this->count('*') ? true : false;
    }

    /**
     * @param string $name
     * @return float
     * @throws Exception
     */
    public function count($name = '*')
    {
        if (!$name) {
            $name = '*';
        }
        if ($name != '*') {
            $name = "[[$name]]";
        }

        $name = "COUNT($name) AS [[n]]";
        $this->select($name);
        $sql = "SELECT {$this->select} FROM {$this->table}";
        $sql2 = $this->collectionSql();
        if ($sql2) {
            $sql .= " $sql2";
        }
        $this->sql = $sql;
        $q = $this->_execute();

        return (float) $q->row['n'];
    }

    /**
     * SUM
     *
     * @param $name
     * @return float
     * @throws Exception
     */
    public function sum($name)
    {
        $this->select("SUM($name) AS [[n]]");
        $this->limit(1);
        $sql = "SELECT {$this->select} FROM {$this->table}";
        if ($sql2 = $this->collectionSql()) {
            $sql .= " $sql2";
        }
        $this->sql = $sql;
        $q = $this->_execute();

        $n = $q->row['n'];
        if ($n === null) {
            $n = 0;
        }

        return (float) $n;
    }

    /**
     * 执行的手工书写的 SQL 语句
     *
     * @param $sql
     * @param array $params
     * @return $this
     */
    public function command($sql, $params = [])
    {
        $this->isCommand = true;
        if ($params) {
            $sql = strtr($sql, $params);
        }
        $this->sql = $sql;

        return $this;
    }

    private function _execute()
    {
        $res = null;
        try {
            $sql = $this->quoteSql($this->sql);
            $message = $sql;
            $q = $this->db->query($sql);

            $res = $q;
        } catch (\Exception $e) {
            $message = $e->getMessage();
            $res = false;
        }
        if ($this->debug) {
            $information = [
                'message' => $message,
                'trace' => [],
            ];
            $traces = debug_backtrace();
            if (isset($traces[1])) {
                $trace = $traces[1];
                $information['trace'] = [
                    'file' => $trace['file'],
                    'line' => $trace['line'],
                ];
            }
            var_dump($information);
        }

        return $res;
    }

    public function execute()
    {
        $this->_execute();

        return $this->db->countAffected();
    }

    /**
     * @return int 最后插入记录 id
     */
    public function getLastInsertId()
    {
        return $this->db->getLastId();
    }

    public function getRawSql()
    {
        return $this->quoteSql($this->sql);
    }

}