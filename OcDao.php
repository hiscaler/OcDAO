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

        return "`$table`";
    }

    private function quoteColumn($column)
    {
        return $this->simpleQuote($column);
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
        if (is_array($condition) && $condition) {
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
        } elseif (!is_string($condition)) {
            throw new Exception("Condition error.");
        }

        if ($params) {
            $where = strtr($where, $params);
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
        $q = $this->db->query($this->sql);

        return $q;
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