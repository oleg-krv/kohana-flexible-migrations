<?php defined('SYSPATH') or die('No direct script access.');

class Drivers_Mysql extends Drivers_Driver
{
  public function __construct($group, $db)
  {
    parent::__construct($group, $db);
    $this->db->query($group, 'START TRANSACTION', false);

  }

  public function __destruct()
  {
    $this->db->query($this->group, 'COMMIT', false);
  }

  /**
   * @param $table_name
   * @param $fields
   * @param bool $primary_key
   * @param bool $engine InnoDB|MyISAM
   * @param bool $default_charset utf8
   * @return array|bool
   */
  public function create_table($table_name, $fields, $primary_key = TRUE, $engine = FALSE, $default_charset = FALSE)
  {
    $sql = "CREATE TABLE `$table_name` (";

    // add a default id column if we don't say not to
    if ($primary_key === TRUE) {
      $primary_key = 'id';
      $fields = array_merge(array('id' => array('integer', 'null' => FALSE)), $fields);
    }

    foreach ($fields as $field_name => $params) {
      $params = (array)$params;

      if ($primary_key === $field_name AND $params[0] == 'integer') {
        $params['auto'] = TRUE;
      }
      $sql .= $this->compile_column($field_name, $params);
      $sql .= ",";
    }

    $sql = rtrim($sql, ',');

    if ($primary_key) {
      $sql .= ' , PRIMARY KEY (';

      foreach ((array)$primary_key as $pk) {
        $sql .= " `$pk`,";
      }
      $sql = rtrim($sql, ',');
      $sql .= ')';
    }

    $sql .= ")";
    if ($engine) {
      switch ($engine) {
        case 'InnoDB': $sql .= ' ENGINE=InnoDB'; break;
        case 'MyISAM': $sql .= ' ENGINE=MyISAM'; break;
      }
    }
    if ($default_charset) {
      if (is_string($default_charset)) {
        $sql .= ' DEFAULT CHARSET='.$default_charset;
      }
    }
    return $this->run_query($sql);
  }

  public function drop_table($table_name)
  {
    return $this->run_query("DROP TABLE $table_name");
  }

  public function rename_table($old_name, $new_name)
  {
    return $this->run_query("RENAME TABLE `$old_name`  TO `$new_name` ;");
  }

  public function add_column($table_name, $column_name, $params)
  {
    $sql = "ALTER TABLE `$table_name` ADD COLUMN " . $this->compile_column($column_name, $params, TRUE);
    return $this->run_query($sql);
  }

  public function rename_column($table_name, $column_name, $new_column_name, $params)
  {
    if ($params == NULL) {
      $params = $this->get_column($table_name, $column_name);
    }
    $sql = "ALTER TABLE `$table_name` CHANGE `$column_name` " . $this->compile_column($new_column_name, $params, TRUE);
    print $sql;
    return $this->run_query($sql);
  }

  public function change_column($table_name, $column_name, $params)
  {
    $sql = "ALTER TABLE `$table_name` MODIFY " . $this->compile_column($column_name, $params);
    return $this->run_query($sql);
  }

  public function remove_column($table_name, $column_name)
  {
    return $this->run_query("ALTER TABLE $table_name DROP COLUMN $column_name ;");
  }

  public function add_index($table_name, $index_name, $columns, $index_type = 'normal')
  {
    switch ($index_type) {
      case 'normal':
        $type = '';
        break;
      case 'unique':
        $type = 'UNIQUE';
        break;
      case 'primary':
        $type = 'PRIMARY';
        break;

      default:
        throw new Exception('migrations.bad_index_type', array($index_type));
    }

    $sql = "ALTER TABLE `$table_name` ADD $type INDEX `$index_name` (";

    foreach ((array)$columns as $column) {
      $sql .= " `$column`,";
    }

    $sql = rtrim($sql, ',');
    $sql .= ')';
    return $this->run_query($sql);
  }

  public function remove_index($table_name, $index_name)
  {
    return $this->run_query("ALTER TABLE `$table_name` DROP INDEX `$index_name`");
  }

  private function reserved_words_default($word)
  {
    switch ($word) {
      case 'CURRENT_TIMESTAMP':
        return true;
      default:
        return false;
    }
  }

  protected function compile_column($field_name, $params, $allow_order = FALSE)
  {
    if (empty($params)) {
      throw new Kohana_Exception('migrations.missing_argument');
    }

    $params = (array)$params;
    $null = FALSE;
    $auto = FALSE;
    $extra = FALSE;
    foreach ($params as $key => $param) {
      $args = NULL;
      if (is_string($key)) {
        switch ($key) {
          case 'after':
            if ($allow_order) $order = "AFTER `$param`";
            break;
          case 'null':
            $null = (bool)$param;
            break;
          case 'unsigned':
            $unsigned = (bool)$param;
            break;
          case 'extra':
            $extra = (string)$param;
            break;
          case 'default':
            if (is_string($param)) {
              if ($this->reserved_words_default($param)) {
                $default = 'DEFAULT ' . $param;
              } else {
                $default = 'DEFAULT ' . $this->db->escape($param);
              }
            } else if (is_bool($param)) {
              if ($param == true) {
                $default = 'DEFAULT 1';
              } else {
                $default = 'DEFAULT 0';
              }
            } elseif (is_null($param)) {
              $default = 'DEFAULT NULL';
              $null = true;
            } else {
              $default = 'DEFAULT ' . $param;
            }
            break;
          case 'auto':
            $auto = (bool)$param;
            break;
          default:
            throw new Kohana_Exception('migrations.bad_column', array($key));
        }
        continue; // next iteration
      }
      // Split into param and args
      if (is_string($param) AND preg_match('/^([^\[]++)\[(.+)\]$/', $param, $matches)) {
        $param = $matches[1];
        $args = $matches[2];

        // Replace escaped comma with comma
        $args = str_replace('\,', ',', $args);
      }
      if ($this->is_type($param)) {
        $type = $this->native_type($param, $args);
        continue;
      }

      switch ($param) {
        case 'first':
          if ($allow_order) $order = 'FIRST';
          continue 2;
        default:
          break;
      }

      throw new Kohana_Exception('migrations.bad_column', $param);
    }

    if (empty($type)) {
      throw new Kohana_Exception('migrations.missing_argument');
    }

    $sql = " `$field_name` $type ";
    isset($unsigned) and $sql .= $unsigned ? ' UNSIGNED ' : '';
    $sql .= $null ? ' NULL ' : ' NOT NULL ';
    isset($default)  and $sql .= " $default ";
    $sql .= $auto ? ' AUTO_INCREMENT ' : '';
    $sql .= $extra ? $extra : '';
    isset($order)    and $sql .= " $order ";

    return $sql;
  }

  protected function get_column($table_name, $column_name)
  {
    //print "SHOW COLUMNS FROM `$table_name` LIKE '$column_name'";
    $result = $this->run_query_result("SHOW COLUMNS FROM `$table_name` LIKE '$column_name'", true);

    if ($result->count() !== 1) {
      throw new Kohana_Exception('migrations.column_not_found', array($column_name, $table_name));
    }

    $result = $result->current();

    $params_type = array($this->migration_type($result->Type));

    $params[] = $params_type[0][0];

    if ($params_type[0][1]) {
      $params['unsigned'] = TRUE;
    }

    if ($result->Null == 'NO') {
      $params['null'] = FALSE;
    }

    if ($result->Default)
      $params['default'] = $result->Default;

    if ($result->Extra == 'auto_increment')
      $params['auto'] = TRUE;

    return $params;
  }

  protected function default_limit($type)
  {
    switch ($type) {
      case 'decimal':
        return "10,0";
      case 'integer':
        return "normal";
      case 'string':
        return "255";
      case 'binary':
        return "1";
      case 'boolean':
        return "1";
      default:
        return "";
    }
  }

  protected function native_type($type, $limit)
  {
    if (!$this->is_type($type)) {
      throw new Kohana_Exception('migrations.unknown_type', array($type));
    }

    if (empty($limit)) {
      $limit = $this->default_limit($type);
    }
    switch ($type) {
      case 'integer':
        if ((int)$limit > 0) {
          return "int($limit)";
        } else {
          switch ($limit) {
            case 'big':
              return 'bigint';
            case 'normal':
              return 'int';
            case 'small':
              return 'smallint';
            default:
              break;
          }
        }
        throw new Kohana_Exception('migrations.unknown_type', array($type));

      case 'string':
        return "varchar ($limit)";
      case 'boolean':
        return 'tinyint (1)';
      default:
        $limit and $limit = "($limit)";
        return "$type $limit";
    }
  }

  protected function migration_null($null)
  {
    if ($null == 'YES') {
      return "'null' => 1";
    } else {
      return false;
    }
  }

  protected function migration_default($default, $null)
  {
    if (is_null($default) AND $null) {
      return "'default' => 'NULL'";
    }
    if (!is_null($default)) {
      return "'default' => '$default'";
    } else {
      return false;
    }
  }

  protected function migration_key($key)
  {
    if ($key != '') {
      if ($key == 'PRI') {
        return true;
      }
    }
    return false;
  }

  protected function migration_extra($extra)
  {
    if ($extra != '') {
      if ($extra == 'auto_increment') {
        return "'auto' => 1";
      } else return "'extra' => '$extra'";
    }
    return false;
  }

  protected function migration_type($native)
  {
    $arr = explode(' ', $native);
    $native = $arr[0];
    $unsigned = false;
    if (isset($arr[1])) {
      if ($arr[1] = 'unsigned') {
        $unsigned = true;
      }
    }
    if (preg_match('/^([^\(]++)\((.+)\)$/', $native, $matches)) {
      $native = $matches[1];
      $limit = $matches[2];
    }

    switch ($native) {
      case 'bigint':
        $return = 'integer[big]';
        break;
      case 'smallint':
        $return = 'integer[small]';
        break;
      case 'int':
        $return = "integer[$limit]";
        break;
      case 'varchar':
        $return = "string[$limit]";
        break;
      case 'tinyint':
        if ($limit == 1) $return = 'boolean';
        else $return = "tinyint[$limit]";
        break;
      case 'text' :
        $return = 'text';
        break;
      case 'datetime':
        $return = 'datetime';
        break;
      case 'timestamp':
        $return = 'timestamp';
        break;
      case 'decimal':
        $return = "decimal[$limit]";
        break;
      default:
        break;
    }
    if (isset($return)) {
      return array($return, $unsigned);
    }
    if (!$this->is_type($native)) {
      throw new Kohana_Exception('migrations.unknown_type', array($native));
    }

    return array($native);
  }


  protected function get_table_dump($table_name, $prefix = false)
  {
    if ($prefix) {
      return $this->run_query_result('SHOW COLUMNS FROM ' . $this->db->table_prefix() . $table_name, false)->as_array();
    } else {
      return $this->run_query_result('SHOW COLUMNS FROM ' . $table_name, false)->as_array();
    }
  }

  /**
   * @return mixed
   */
  protected function get_tables()
  {
    return $this->run_query_result("SHOW TABLES LIKE '" . $this->db->table_prefix() . "%'", false)->as_array();
  }

  /**
   * @param $table
   * @return mixed
   */
  protected function get_table_info($table)
  {
    $database = $this->config_db['connection']['database'];
    $query = "select engine, CHARACTER_SETS.CHARACTER_SET_NAME as charset "
      . "from information_schema.tables "
      . "join information_schema.CHARACTER_SETS "
      . "on tables.TABLE_COLLATION = CHARACTER_SETS.DEFAULT_COLLATE_NAME "
      ."where table_name=" . $this->db->escape($table)
      . " and TABLE_SCHEMA=" .$this->db->escape($database);
    return $this->run_query_result($query)->current();
  }

  /**
   * @param $table
   * @return string
   */
  protected function get_table_index($table)
  {
    $indexes = $this->run_query_result("SHOW INDEX FROM $table")->as_array();
    $res_indexes = array();
    foreach ($indexes as $index) {
      if ($index['Key_name'] != 'PRIMARY') {
        $res_indexes[$index['Key_name']]['columns'][] = $index['Column_name'];
        if ($index['Non_unique']) {
          $res_indexes[$index['Key_name']]['type'] = 'unique' ;
        } else {
          $res_indexes[$index['Key_name']]['type'] = 'normal' ;
        }
      }
    }
    $res = '';
    foreach ($res_indexes as $key => $value){
      $res .= "\t\t" . '$this->add_index(' . "'$table', ";
      $res .= "'$key',\n\t\t\tarray(";
      $res_column = '';
      foreach ($value['columns'] as $column) {
        $res_column .= "'$column', ";
      }
      $res .= substr($res_column,0, strripos($res_column,','));
      $res .= "),";
      $res .= "'" . $value['type'] . "'";
      $res .= ");\n";
    }
    if ($table == 'ts_files') {
    }
    return $res;
  }

  /**
   * Create dump base
   * @return array
   */
  public function dump(){
    $tables = $this->get_tables();
    $commands = array();
    foreach ($tables as $key => $value) {
      foreach ($value as $key => $value2) {
        $fields = $this->get_table_dump($value2, false);
        $command = "\t\t".'$this->create_table(' . "'" . $value2 . "', array(\n";
        $primary_keys= false;
        foreach ($fields as $field) {
          $type = $this->migration_type($field['Type']);
          if ($type[1]) {
            $unsigned = "'unsigned' => 1";
          } else {
            $unsigned = false;
          }
          $null = $this->migration_null($field['Null']);
          $default = $this->migration_default($field['Default'], (bool)$null);
          if ($this->migration_key($field['Key'])) {
            $primary_keys[] = $field['Field'];
          };

          $extra = $this->migration_extra($field['Extra']);
          $command .= "\t\t\t'" . $field['Field'] .  "' => array('" . $type[0] . "'";
          if ($null) $command .= ",\n\t\t\t\t" . $null;
          if ($unsigned) $command .= ",\n\t\t\t\t" . $unsigned;
          if ($default) $command .= ",\n\t\t\t\t" . $default;
          if ($extra) $command .= ",\n\t\t\t\t" . $extra;
          $command .= "),\n";
        }
        $command = substr($command,0, strripos($command,',')) . ",\n\t\t\t),";
        $table_info = $this->get_table_info($value2);
        if ($primary_keys) {
          $command_primary_key = " array(";
          foreach ($primary_keys as $primary_key){
            $command_primary_key .= "'" . $primary_key . "', ";
          }
          $command .= substr($command_primary_key,0, strripos($command_primary_key,',')) . '),';
        } else {
          $command .= 'false,';
        }
        $command .= "'" . $table_info['engine'] ."',";
        $command .= "'" . $table_info['charset'] ."'";
        $command .= ");\n";
        $index = $this->get_table_index($value2);
        if ($index)
          $command .= "\n$index\n";
        $commands['up'][] = $command;
        $commands['down'][] = "\t\t".'$this->drop_table('."'" . $value2 . "'" . ");\n";
      }
    }
    return $commands;
  }
}