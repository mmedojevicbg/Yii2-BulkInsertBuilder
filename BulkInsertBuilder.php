<?php

namespace mmedojevicbg\Yii2-BulkInsertBuilder;

class BulkInsertBuilder {
    const STATE_NEW = 0;
    const STATE_APPENDING = 1;

    protected $table;
    protected $insertFieldsQueryPart;
    protected $updateFieldsArray;
    protected $bulkSize;
    protected $query;
    protected $count;
    protected $state;

    public function __construct($table, $insertFieldsQueryPart, $updateFieldsArray = false, $bulkSize = 1000) {
        $this->table = $table;
        $this->insertFieldsQueryPart = $insertFieldsQueryPart;
        $this->updateFieldsArray = $updateFieldsArray;
        $this->bulkSize = $bulkSize;
        $this->count = 0;
        $this->buildQueryFirstPart();
    }

    public function insert($valuesQueryPart) {
        $this->appendValues($valuesQueryPart);
        $this->count++;
        if($this->count == $this->bulkSize) {
            $this->count = 0;
            $this->removeTrailingComma();
            $this->appendOnDuplicateUpdate();
            $this->executeQuery();
            $this->buildQueryFirstPart();
        }
    }

    public function flush() {
        if($this->state == self::STATE_APPENDING) {
            $this->removeTrailingComma();
            $this->appendOnDuplicateUpdate();
            $this->executeQuery();
        }
    }

    protected function buildQueryFirstPart() {
        $this->query = 'INSERT INTO ' . $this->table . ' (' . $this->insertFieldsQueryPart . ') VALUES';
        $this->state = self::STATE_NEW;
    }

    protected function appendValues($valuesQueryPart) {
        $this->query .= '(' . $valuesQueryPart . '),';
        $this->state = self::STATE_APPENDING;
    }

    protected function removeTrailingComma() {
        $this->query = trim($this->query, ',');
    }

    protected function appendOnDuplicateUpdate() {
        if($this->updateFieldsArray) {
            $this->query .= ' ON DUPLICATE KEY UPDATE ';
            foreach($this->updateFieldsArray as $field) {
                $this->query .=  $field . ' = VALUES(' . $field . '),';
            }
            $this->removeTrailingComma();
        }
    }

    protected function executeQuery() {
        Yii::$app->db->createCommand($this->query)->query();
    }
} 