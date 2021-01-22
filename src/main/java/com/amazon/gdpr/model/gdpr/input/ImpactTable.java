package com.amazon.gdpr.model.gdpr.input;

public class ImpactTable {

	int impactTableId;
	String impactSchema;
	String impactTableName;
	String impactColumns;
	String impactTableType;
	String parentSchema;
	String parentTable;
	String impactTableColumn;
	String parentTableColumn;
		
	/**
	 * @param impactTableId
	 * @param impactSchema
	 * @param impactTableName
	 * @param impactTableType
	 * @param parentSchema
	 * @param parentTable
	 * @param impactTableColumn
	 * @param parentTableColumn
	 * @param condition
	 */
	public ImpactTable(int impactTableId, String impactSchema, String impactTableName, String impactColumns, 
			String impactTableType, String parentSchema, String parentTable, String impactTableColumn, 
			String parentTableColumn) {
		super();
		this.impactTableId = impactTableId;
		this.impactSchema = impactSchema;
		this.impactTableName = impactTableName;
		this.impactColumns = impactColumns;
		this.impactTableType = impactTableType;
		this.parentSchema = parentSchema;
		this.parentTable = parentTable;
		this.impactTableColumn = impactTableColumn;
		this.parentTableColumn = parentTableColumn;
	}
	/**
	 * @param impactTableId
	 * @param impactTableName
	 * @param parentTable
	 * @param impactColumnMapped
	 * @param parentColumnMapped
	 */
	public ImpactTable(int impactTableId, String impactTableName, String parentTable, String impactTableColumn,
			String parentTableColumn) {
		super();
		this.impactTableId = impactTableId;
		this.impactTableName = impactTableName;
		this.parentTable = parentTable;
		this.impactTableColumn = impactTableColumn;
		this.parentTableColumn = parentTableColumn;
	}
	/**
	 * @return the impactTableId
	 */
	public int getImpactTableId() {
		return impactTableId;
	}
	/**
	 * @param impactTableId the impactTableId to set
	 */
	public void setImpactTableId(int impactTableId) {
		this.impactTableId = impactTableId;
	}
	/**
	 * @return the impactSchema
	 */
	public String getImpactSchema() {
		return impactSchema;
	}
	/**
	 * @param impactSchema the impactSchema to set
	 */
	public void setImpactSchema(String impactSchema) {
		this.impactSchema = impactSchema;
	}
	/**
	 * @return the impactTableName
	 */
	public String getImpactTableName() {
		return impactTableName;
	}
	/**
	 * @param impactTableName the impactTableName to set
	 */
	public void setImpactTableName(String impactTableName) {
		this.impactTableName = impactTableName;
	}	
	/**
	 * @return the impactColumns
	 */
	public String getImpactColumns() {
		return impactColumns;
	}
	/**
	 * @param impactColumns the impactColumns to set
	 */
	public void setImpactColumns(String impactColumns) {
		this.impactColumns = impactColumns;
	}
	/**
	 * @return the impactTableType
	 */
	public String getImpactTableType() {
		return impactTableType;
	}
	/**
	 * @param impactTableType the impactTableType to set
	 */
	public void setImpactTableType(String impactTableType) {
		this.impactTableType = impactTableType;
	}
	/**
	 * @return the parentSchema
	 */
	public String getParentSchema() {
		return parentSchema;
	}
	/**
	 * @param parentSchema the parentSchema to set
	 */
	public void setParentSchema(String parentSchema) {
		this.parentSchema = parentSchema;
	}
	/**
	 * @return the parentTable
	 */
	public String getParentTable() {
		return parentTable;
	}
	/**
	 * @param parentTable the parentTable to set
	 */
	public void setParentTable(String parentTable) {
		this.parentTable = parentTable;
	}
	/**
	 * @return the impactTableColumn
	 */
	public String getImpactTableColumn() {
		return impactTableColumn;
	}
	/**
	 * @param impactTableColumn the impactTableColumn to set
	 */
	public void setImpactTableColumn(String impactTableColumn) {
		this.impactTableColumn = impactTableColumn;
	}
	/**
	 * @return the parentTableColumn
	 */
	public String getParentTableColumn() {
		return parentTableColumn;
	}
	/**
	 * @param parentTableColumn the parentTableColumn to set
	 */
	public void setParentTableColumn(String parentTableColumn) {
		this.parentTableColumn = parentTableColumn;
	}
}