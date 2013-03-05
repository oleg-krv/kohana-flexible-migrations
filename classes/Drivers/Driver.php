<?php defined('SYSPATH') or die('No direct script access.');

abstract class Drivers_Driver
{
	/**
	 * Valid types
	 * @var array
	 */
	protected $types = array
	(
		'decimal',
		'float',
		'integer',
		'datetime',
		'date',
		'timestamp',
		'time',
		'text',
		'string',
		'binary',
		'boolean',
	);
	
	/**
	 * @var Database
	 */
	protected $db;

	protected $group;

  /**
   * Copy database object
   *
   * @param $group
   * @param $db
   */
	public function __construct($group, $db)
	{
		$this->group = $group;
		$this->db    = $db;
		$this->types = array_combine($this->types, $this->types);
    $name = Database::$default;
    $this->config_db = Kohana::$config->load('database')->$name;
	}

  /**
   * Create Table
   *
   * Creates a new table
   *
   * $fields:
   *
   *     Associative array containing the name of the field as a key and the
   *     value could be either a string indicating the type of the field, or an
   *     array containing the field type at the first position and any optional
   *     arguments the field might require in the remaining positions.
   *     Refer to the TYPES function for valid type arguments.
   *     Refer to the FIELD_ARGUMENTS function for valid optional arguments for a
   *     field.
   *
   * @example
   *
   *    create_table (
   *       'blog',
   *       array (
   *         'title' => array ( 'string[50]', 'default' => "The blog's title." ),
   *         'date' => 'date',
   *         'content' => 'text'
   *       )
   *     )
   *
   * @param $table_name string of the table to be created
   * @param $fields array
   * @param bool $primary_key mixed key, false if not desired, not specified sets to 'id' column.
   *                                     Will be set to auto_increment by default.
   * @param bool $engine
   * @param bool $default_charset
   * @return  boolean
   */
	abstract public function create_table($table_name, $fields, $primary_key = TRUE, $engine = FALSE, $default_charset = FALSE);

	/**
	 * Drop a table
	 *
	 * @param string    Name of the table
	 * @return boolean
	 */
	abstract public function drop_table($table_name);

	/**
	 * Rename a table
	 *
	 * @param   string    Old table name
	 * @param   string    New name
	 * @return  boolean
	 */
	abstract public function rename_table($old_name, $new_name);
	
	/**
	 * Add a column to a table
	 *
	 * @example add_column ( "the_table", "the_field", array('string[25]', 'null' => FALSE) );
	 * @example add_coumnn ( "the_table", "int_field", "integer" );
	 *
	 * @param   string  Name of the table
	 * @param   string  Name of the column
	 * @param   array   Column arguments array, or just a type for a simple column
	 * @return  bool
	 */
	abstract public function add_column($table_name, $column_name, $params);
	
	/**
	 * Rename a column
	 *
	 * @param   string  Name of the table
	 * @param   string  Name of the column
	 * @param   string  New name
	 * @return  bool
	 */
	abstract public function rename_column($table_name, $column_name, $new_column_name, $params);
	
	/**
	 * Alter a column
	 *
	 * @param   string  Table name
	 * @param   string  Columnn ame
	 * @param   string  New column type
	 * @param   array   Column argumetns
	 * @return  bool
	 */
	abstract public function change_column($table_name, $column_name, $params);
	
	/**
	 * Remove a column from a table
	 *
	 * @param   string  Name of the table
	 * @param   string  Name of the column
	 * @return  bool
	 */
	abstract public function remove_column($table_name, $column_name);

	/**
	 * Add an index
	 *
	 * @param   string  Name of the table
	 * @param   string  Name of the index
	 * @param   string|array  Name(s) of the column(s)
	 * @param   string  Type of the index (unique/normal/primary)
	 * @return  bool
	 */
	abstract public function add_index($table_name, $index_name, $columns, $index_type = 'normal');

	/**
	 * Remove an index
	 *
	 * @param   string  Name of the table
	 * @param   string  Name of the index
	 * @return  bool
	 */
	abstract public function remove_index($table_name, $index_name);

  /**
   * Catch query exceptions
   *
   * @param $sql
   * @param bool $type
   * @return bool|array|object
   */
	public function run_query($sql, $type = false)
	{
		try
		{
    	$test = $this->db->query($this->group, $sql, false);
		}
		catch (Kohana_Database_Exception $e)
		{
			// Kohana::log('error', 'Migration Failed: ' . $e);
			echo $e->getMessage();
			exit();
			//return FALSE;
		}
    if (!$type) {
		  return TRUE;
    } else {
      return $test;
    }
	}

  public function run_query_result($sql, $as_object = false){
    $test = $this->db->query(Database::SELECT, $sql, $as_object);
    return $test;
  }

  /**
   * Is this a valid type?
   *
   * @param $type
   * @return bool
   */
	protected function is_type($type)
	{
		return isset($this->types[$type]);
	}

  abstract protected function get_table_dump($table_name, $prefix = false);
  abstract protected function get_table_info($table);
  abstract protected function get_tables();
  abstract protected function migration_type($native);
  abstract protected function migration_null($native);
  abstract protected function migration_default($default, $null);
  abstract protected function migration_key($key);
  abstract protected function migration_extra($extra);
  abstract public function dump();

}
