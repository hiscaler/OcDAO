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

    private $table;
    private $select = '*';
    private $selectSet = [];
    private $where;
    private $offset = 0;
    private $limit = null;
    private $orderBy;
    private $leftJoin;
    protected $tableQuoteCharacter = "`";
    protected $columnQuoteCharacter = '`';

    /**
     * @var string SQL
     */
    private $sql;

    public function __construct($registry)
    {
        $this->db = $registry->get('db');
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

    private function quoteColumnName($name)
    {
        if (strpos($name, '(') !== false || strpos($name, '[[') !== false) {
            return $name;
        }
        if (($pos = strrpos($name, '.')) !== false) {
            $prefix = $this->quoteTableName(substr($name, 0, $pos)) . '.';
            $name = substr($name, $pos + 1);
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

    public function quoteSql($sql)
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

    public function insert($table, $columns)
    {
        $sql = "INSERT INTO %s";
        $fields = [];
        $values = [];
        foreach ($columns as $column => $value) {
            $fields[] = $this->quoteColumnName($column);
            $values[] = $this->quoteValue($value);
        }
        $sql .= " (" . implode(', ', $fields) . ")";
        $sql .= " VALUES (" . implode(', ', $values) . ")";

        $this->sql = sprintf($sql, $this->quoteTableName($table));

        return $this;
    }

    /**
     * id = 1
     * id IN (1,2)
     *
     * @param $condition
     * @param $params
     * @return string
     * @throws Exception
     */
    private function buildCondition($condition, $params)
    {
        $where = '';
        if (is_array($condition)) {
            if ($condition) {
                $ws = [];
                foreach ($condition as $key => $value) {
                    $s = $this->quoteColumnName($key);
                    if (is_array($value)) {
                        $valueList = [];
                        foreach ($value as $v) {
                            if (is_numeric($v)) {
                                $valueList[] = $v;
                            } else {
                                $valueList[] = $this->quoteValue($v);
                            }
                        }
                        $s .= ' IN (' . implode(", ", $valueList) . ')';
                    } else {
                        $s .= ' = ' . (is_numeric($value) ? $value : $this->quoteValue($value));
                    }
                    $ws[] = $s;
                }
                $where = implode(" AND ", $ws);
            }
        } elseif (is_null($condition)) {
            $where = '';
        } elseif (is_string($condition)) {
            $where = $condition;
        } elseif (!is_string($condition)) {
            throw new Exception("Condition error.");
        }

        if ($params) {
            $where = strtr($condition, $params);
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
            $where = $this->buildCondition($condition, $params);
            if ($where) {
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
        $where = $this->buildCondition($condition, $params);
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

    public function select($select)
    {
        $this->_select($select, false);

        return $this;
    }

    public function addSelect($select)
    {
        $this->_select($select, true);

        return $this;
    }

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
    public function where($condition, $params)
    {
        $this->where = $this->buildCondition($condition, $params);

        return $this;
    }

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

    public function offset($n)
    {
        $n = (int) $n;
        $this->offset = $n < 0 ? 0 : $n;

        return $this;
    }

    public function limit($n)
    {
        $n = (int) $n;
        $this->limit = $n < 0 ? 0 : $n;

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
        $where = $this->buildCondition($condition, $params);
        if ($where) {
            $sql .= ' ON ' . $where;
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
        $sql = "SELECT {$this->select} FROM {$this->table}";
        if ($this->leftJoin) {
            $sql .= " $this->leftJoin";
        }
        if ($this->where) {
            $sql .= " WHERE {$this->where}";
        }
        if ($this->orderBy) {
            $sql .= " ORDER BY {$this->orderBy}";
        }
        $this->limit(1);
        $sql .= " LIMIT $this->offset, $this->limit";
        $this->sql = $sql;
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
        $sql = "SELECT {$this->select} FROM {$this->table}";
        if ($this->leftJoin) {
            $sql .= " $this->leftJoin";
        }
        if ($this->where) {
            $sql .= " WHERE {$this->where}";
        }
        if ($this->orderBy) {
            $sql .= " ORDER BY {$this->orderBy}";
        }
        if ($this->limit) {
            $sql .= " LIMIT $this->offset, $this->limit";
        }

        $this->sql = $sql;
        $q = $this->_execute();

        return $q === false ? [] : $q->rows;
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
        $rows = $this->all();
        foreach ($rows as $row) {
            foreach ($row as $k => $v) {
                if ($k == 0) {
                    $res[] = $v;
                    break;
                }
            }
        }

        return $res;
    }

    /**
     * @param $table
     * @param string $condition
     * @param array $params
     * @return boolean
     * @throws Exception
     */
    public function exist($table, $condition = '', $params = [])
    {
        return $this->count($table, $condition, $params) ? true : false;
    }

    /**
     * @param $table
     * @param string $condition
     * @param array $params
     * @return float
     * @throws Exception
     */
    public function count($table, $condition = '', $params = [])
    {
        $sql = 'SELECT COUNT(*) AS ' . $this->quoteColumnName('n') . ' FROM ' . $this->quoteTableName($table);
        $where = $this->buildCondition($condition, $params);
        $where && $sql .= " WHERE $where";
        $this->sql = $sql;
        $q = $this->_execute();

        return (float) $q->row['n'];
    }

    /**
     * SUM
     *
     * @param $table
     * @param $field
     * @param string $condition
     * @param array $params
     * @return float
     * @throws Exception
     */
    public function sum($table, $field, $condition = '', $params = [])
    {
        $sql = 'SELECT SUM(' . $this->quoteColumnName($field) . ') AS ' . $this->quoteColumnName('n') . ' FROM ' . $this->quoteTableName($table);
        $where = $this->buildCondition($condition, $params);
        $where && $sql .= " WHERE $where";
        $this->sql = $sql;
        $q = $this->_execute();
        $n = $q->row['n'];
        if ($n === null) {
            $n = 0;
        }

        return (float) $n;
    }

    private function _execute()
    {
        try {
            $sql = $this->quoteSql($this->sql);
            $q = $this->db->query($sql);

            return $q;
        } catch (\Exception $e) {
            return false;
        }
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