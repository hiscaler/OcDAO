<?php

/**
 * OpenCart Database Access Objects
 *
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
    private $where;
    private $offset = 0;
    private $limit = null;
    private $orderBy;

    /**
     * @var string SQL
     */
    private $sql;

    public function __construct($registry)
    {
        $this->db = $registry->get('db');
    }

    private function simpleQuote($v)
    {
        return "`$v`";
    }

    private function quoteTable($table)
    {
        $tablePrefix = DB_PREFIX;
        if (stripos($table, $tablePrefix) === false) {
            $table = $tablePrefix . $table;
        }
        if (strpos($table, '`') === false) {
            $table = "`$table`";
        }

        return $table;
    }

    private function quoteColumn($column)
    {
        if (strpos($column, '`') === false) {
            $column = $this->simpleQuote($column);
        }

        return $column;
    }

    private function quoteValue($value)
    {
        return "'$value'";
    }

    public function insert($table, $columns)
    {
        $sql = "INSERT INTO %s";
        $fields = [];
        $values = [];
        foreach ($columns as $column => $value) {
            $fields[] = $this->quoteColumn($column);
            $values[] = $this->quoteValue($value);
        }
        $sql .= " (" . implode(', ', $fields) . ")";
        $sql .= " VALUES (" . implode(', ', $values) . ")";

        $this->sql = sprintf($sql, $this->quoteTable($table));

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
                    $s = $this->quoteColumn($key);
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
        } elseif (!is_string($condition)) {
            throw new Exception("Condition error.");
        }

        if ($params) {
            $where = strtr($where, $params);
        }

        return $where;
    }

    private function parseOrderBy($orderBy)
    {
        $s = [];
        if ($orderBy) {
            if (is_array($orderBy)) {
                foreach ($orderBy as $k => $v) {
                    $s[] = $this->quoteColumn($k) . ($v == SORT_DESC ? ' DESC' : ' ASC');
                }
            }
        }

        $this->orderBy = $s ? implode(', ', $s) : '';
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
                $sets[] = self::quoteColumn($k) . ' = ' . $this->quoteValue($v);
            }
            $sql = "UPDATE " . $this->quoteTable($table) . " SET " . implode(", ", $sets);
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
        $sql = "DELETE FROM " . $this->quoteTable($table);
        $where = $this->buildCondition($condition, $params);
        $where && $sql .= " WHERE $where";
        $this->sql = $sql;

        return $this;
    }

    public function select($select)
    {
        if ($select && is_string($select)) {
            $select = explode(",", $select);
        } elseif (!is_array($select)) {
            $select = [];
        }
        if ($select) {
            $ss = [];
            foreach ($select as $v) {
                $v = trim($v);
                $ss[] = $v != '*' ? $this->quoteColumn($v) : $v;
            }
            $this->select = implode(", ", $ss);
        }

        return $this;
    }

    public function from($table)
    {
        $this->table = $this->quoteTable($table);

        return $this;
    }

    /**
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
                    $s[] = $this->quoteColumn($k) . ($v == SORT_DESC ? ' DESC' : ' ASC');
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
     * 返回一条记录
     *
     * @return mixed
     * @throws Exception
     */
    public function one()
    {
        $sql = "SELECT {$this->select} FROM {$this->table}";
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
        $sql = 'SELECT COUNT(*) AS ' . $this->quoteColumn('n') . ' FROM ' . $this->quoteTable($table);
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
        $sql = 'SELECT SUM(' . $this->quoteColumn($field) . ') AS ' . $this->quoteColumn('n') . ' FROM ' . $this->quoteTable($table);
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
            $q = $this->db->query($this->sql);

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
        return $this->sql;
    }

}