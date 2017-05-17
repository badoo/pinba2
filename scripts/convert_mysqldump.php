<?php
/**
 * Simple MySQL dump converter from first version
 *
 * NOTE: it uses regular expressions, so it works only with original MySQL dump format with single-quoted comments
 *
 * NOTE: all reports will be created with default_history_time, so they will use whatever time is configured in my.cnf
 *
 * Usage:
 * > php scripts/convert_mysqldump.php dump.sql
 *
 * @author Oleg Efimov <efimovov@gmail.com>
 */

/* ===== Check arguments ===== */

if ($GLOBALS['argc'] !== 2) {
    showUsageAndExit(1);
}

$filename = $GLOBALS['argv'][1];
if (!is_readable($filename)) {
    showUsageAndExit(2);
}

/* ===== Extract CREATE TABLE queries and convert them ===== */

$sql = file_get_contents($filename);

$result = preg_match_all("/CREATE TABLE.*?ENGINE=PINBA.*?COMMENT='.*?';/s", $sql, $matches);
if ($result === false) {
    showUsageAndExit(3);
}

foreach ($matches[0] as $match) {
    convertTableSql($sql, $match);
}

echo $sql;
exit;

/* ===== Functions ===== */

function showUsageAndExit($exit_code)
{
    echo "Usage:\n";
    echo "> php scripts/convert_mysqldump.php dump.sql\n";
    exit($exit_code);
}

function convertTableSql(&$full_sql, $create_table_sql)
{
    $result = preg_match("/COMMENT='(.*?)';/", $create_table_sql, $matches);
    if ($result === false) {
        return;
    }

    $comment = $matches[1];

    if (tryConvertDefaultTableSql($full_sql, $create_table_sql, $comment)) {
        return;
    }
}

function tryConvertDefaultTableSql(&$full_sql, $create_table_sql, $comment)
{
    $filename = "default_tables/{$comment}.sql";
    if (!is_readable($filename)) {
        return false;
    }

    $default_table_sql = file_get_contents($filename);

    if (!preg_match("/CREATE TABLE.*?\((.*)\)[^)]*COMMENT='(.*?)';/s", $default_table_sql, $matches)) {
        showUsageAndExit(4);
    }

    $default_table_sql_body = $matches[1];
    $default_table_sql_comment = $matches[2];

    if (!preg_match("/(CREATE TABLE.*?\()(.*)(\)[^)]*COMMENT=')(?:.*?)(';)/s", $create_table_sql, $matches)) {
        showUsageAndExit(5);
    }

    $create_table_sql_converted = $matches[1] . $default_table_sql_body . $matches[3] . $default_table_sql_comment . $matches[4];

    $full_sql = str_replace($create_table_sql, $create_table_sql_converted, $full_sql);

    return true;
}
