package com.amazon.gdpr.model.gdpr.output;

public class BackupTableDetails {

	String backupTableName;
	String backupTablecolumn;
	
	/**
	 * @param backupTableName
	 * @param backupTablecoulmn
	 */
	public BackupTableDetails(String backupTableName,
			String backupTablecolumn) {
		this.backupTableName = backupTableName;
		this.backupTablecolumn = backupTablecolumn;
		
	}

	
	
	public String getBackupTableName() {
		return backupTableName;
	}



	public void setBackupTableName(String backupTableName) {
		this.backupTableName = backupTableName;
	}



	public String getBackupTablecolumn() {
		return backupTablecolumn;
	}



	public void setBackupTablecolumnn(String backupTablecolumn) {
		this.backupTablecolumn = backupTablecolumn;
	}



	@Override
	public String toString() {
		return (this.backupTableName  +" "+ this.backupTablecolumn );
	}	
}