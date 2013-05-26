<?php
/**
 * Insert Returning for PostgreSQL
 *
 * @link      http://devpreview.ru
 * @copyright Copyright (c) 2013 Alexey Savchuk. (http://www.devpreview.com)
 * @license   http://framework.zend.com/license/new-bsd New BSD License
 */

namespace ZendExtensions\Db\Sql;

use Zend\Db\Sql\Insert as ZendInsert;
use Zend\Db\Sql\TableIdentifier;
use Zend\Db\Sql\Expression;
use Zend\Db\Adapter\AdapterInterface;
use Zend\Db\Adapter\ParameterContainer;
use Zend\Db\Adapter\Platform\PlatformInterface;
use Zend\Db\Adapter\Platform\Sql92;
use Zend\Db\Adapter\StatementContainerInterface;

class Insert extends ZendInsert
{
	/**
     * @var array
     */
    protected $returningColumns = array();

    /**
     * @var array Specification array
     */
    protected $specifications = array(
        self::SPECIFICATION_INSERT => 'INSERT INTO %1$s (%2$s) VALUES (%3$s) RETURNING %4$s'
    );
    
	/**
     * Specify returning columns
     *
     * @param  array $returningColumns
     * @return Insert
     */
    public function returningColumns(array $returningColumns)
    {
        $this->returningColumns = $returningColumns;
        return $this;
    }
    
    /**
     * Prepare statement
     *
     * @param  AdapterInterface $adapter
     * @param  StatementContainerInterface $statementContainer
     * @return void
     */
    public function prepareStatement(AdapterInterface $adapter, StatementContainerInterface $statementContainer)
    {
        $driver   = $adapter->getDriver();
        $platform = $adapter->getPlatform();
        $parameterContainer = $statementContainer->getParameterContainer();

        if (!$parameterContainer instanceof ParameterContainer) {
            $parameterContainer = new ParameterContainer();
            $statementContainer->setParameterContainer($parameterContainer);
        }

        $table = $this->table;
        $schema = null;

        // create quoted table name to use in insert processing
        if ($table instanceof TableIdentifier) {
            list($table, $schema) = $table->getTableAndSchema();
        }

        $table = $platform->quoteIdentifier($table);

        if ($schema) {
            $table = $platform->quoteIdentifier($schema) . $platform->getIdentifierSeparator() . $table;
        }

        $columns = array();
        $values  = array();

        foreach ($this->columns as $cIndex => $column) {
            $columns[$cIndex] = $platform->quoteIdentifier($column);
            if (isset($this->values[$cIndex]) && $this->values[$cIndex] instanceof Expression) {
                $exprData = $this->processExpression($this->values[$cIndex], $platform, $driver);
                $values[$cIndex] = $exprData->getSql();
                $parameterContainer->merge($exprData->getParameterContainer());
            } else {
                $values[$cIndex] = $driver->formatParameterName($column);
                if (isset($this->values[$cIndex])) {
                    $parameterContainer->offsetSet($column, $this->values[$cIndex]);
                } else {
                    $parameterContainer->offsetSet($column, null);
                }
            }
        }

    	$returningColumns = array();

        foreach ($this->returningColumns as $columnIndexOrAs => $column) {
            if (is_string($columnIndexOrAs)) {
                $returningColumns[] = $platform->quoteIdentifier($column)
                	. ' as ' . $platform->quoteIdentifier($columnIndexOrAs);
            } else {
            	$returningColumns[] = $platform->quoteIdentifier($column);
            }
        }

        if(!$this->returningColumns) {
        	$returningColumns[] = 'NULL';
        }
        
        $returningColumns = implode(', ', $returningColumns);
        
        $sql = sprintf(
            $this->specifications[self::SPECIFICATION_INSERT],
            $table,
            implode(', ', $columns),
            implode(', ', $values),
            $returningColumns
        );

        $statementContainer->setSql($sql);
    }

    /**
     * Get SQL string for this statement
     *
     * @param  null|PlatformInterface $adapterPlatform Defaults to Sql92 if none provided
     * @return string
     */
    public function getSqlString(PlatformInterface $adapterPlatform = null)
    {
        $adapterPlatform = ($adapterPlatform) ?: new Sql92;
        $table = $this->table;
        $schema = null;

        // create quoted table name to use in insert processing
        if ($table instanceof TableIdentifier) {
            list($table, $schema) = $table->getTableAndSchema();
        }

        $table = $adapterPlatform->quoteIdentifier($table);

        if ($schema) {
            $table = $adapterPlatform->quoteIdentifier($schema) . $adapterPlatform->getIdentifierSeparator() . $table;
        }

        $columns = array_map(array($adapterPlatform, 'quoteIdentifier'), $this->columns);
        $columns = implode(', ', $columns);

        $values = array();
        foreach ($this->values as $value) {
            if ($value instanceof Expression) {
                $exprData = $this->processExpression($value, $adapterPlatform);
                $values[] = $exprData->getSql();
            } elseif ($value === null) {
                $values[] = 'NULL';
            } else {
                $values[] = $adapterPlatform->quoteValue($value);
            }
        }

        $values = implode(', ', $values);

        $returningColumns = array();

        foreach ($this->returningColumns as $columnIndexOrAs => $column) {
            if (is_string($columnIndexOrAs)) {
                $returningColumns[] = $adapterPlatform->quoteIdentifier($column)
                	. ' as ' . $adapterPlatform->quoteIdentifier($columnIndexOrAs);
            } else {
            	$returningColumns[] = $adapterPlatform->quoteIdentifier($column);
            }
        }

        if(!$this->returningColumns) {
        	$returningColumns[] = 'NULL';
        }
        
        $returningColumns = implode(', ', $returningColumns);
        
        return sprintf(
        	$this->specifications[self::SPECIFICATION_INSERT],
        	$table,
        	$columns,
        	$values,
        	$returningColumns
        );
    }
}
