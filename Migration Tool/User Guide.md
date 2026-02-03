# **Zendesk Migration Toolkit**

This toolkit automates the migration of **Ticket Fields**, **User Fields**, **Organization Fields**, and **Ticket Forms** between Zendesk instances. It supports two workflows:

1. **Direct Migration:** API-to-API transfer.  
2. **Offline Curation:** Export to CSV, manually select/edit fields, and import to the destination.

## **📂 Project Structure**

* migration.py: The main engine. Analyze differences, export CSV plans, or perform direct migrations.  
* import\_from\_csv.py: Imports a curated CSV file into the target instance.  
* undo\_migration.py: The safety net. Deletes items created during the last migration or import.  
* config.json: Stores API credentials and configuration settings.

## **⚙️ Setup**

1. Install Dependencies:  
   Ensure you have Python installed, then install the required libraries:  
   pip install requests tqdm

2. Configuration:  
   Open config.json and configure your Source and Target credentials.  
   * **Note:** The rollback\_filename is set to "rollback\_log.csv" to ensure consistency across all scripts.

{  
    "source\_creds": {  
        "subdomain": "source-subdomain",  
        "email": "admin@source.com",  
        "token": "YOUR\_SOURCE\_API\_TOKEN"  
    },  
    "target\_creds": {  
        "subdomain": "target-subdomain",  
        "email": "admin@target.com",  
        "token": "YOUR\_TARGET\_API\_TOKEN"  
    },  
    "rollback\_filename": "rollback\_log.csv"  
}

## **🚀 Workflow 1: Offline Curation (Recommended)**

Use this method if you want to review, clean up, or select specific fields before migrating.

1. Generate the Plan:  
   Run the migration script:  
   python migration.py

   Select option **(C)reate CSV**. Enter a filename (e.g., migration\_plan.csv).  
2. Curate Data:  
   Open the CSV in Excel or a text editor:  
   * **Delete rows** for fields or forms you do *not* want to migrate.  
   * **Note:** If you delete a Field row, ensure you delete its associated "option" rows beneath it.  
3. Import:  
   Run the import script:  
   python import\_from\_csv.py

   Enter the path to your edited CSV file. The script will create the objects in the target instance and log them to rollback\_log.csv.

## **⚡ Workflow 2: Direct Migration**

Use this method to sync everything from Source to Target immediately.

1. Run the migration script:  
   python migration.py

2. Select option **(M)igrate Data**.  
3. The script will:  
   * Compare Source and Target to avoid duplicates.  
   * Create missing Fields and Forms.  
   * Log all created IDs to rollback\_log.csv.

## **↩️ Rollback (Undo)**

If an error occurs or you need to revert changes, you can undo the last operation.

1. Ensure rollback\_log.csv exists in the script directory.  
2. Run the undo script:  
   python undo\_migration.py

3. Type DELETE to confirm. The script will remove items in reverse order (LIFO) to preserve dependencies.

## **⚠️ Important Notes**

* **API Rate Limits:** The scripts handle HTTP 429 errors automatically by pausing execution.  
* **Security:** config.json contains plain text API tokens. Do not commit this file to public repositories.  
* **Dependencies:** Ticket Forms rely on Ticket Fields. The scripts automatically handle this mapping (replacing Source IDs with new Target IDs).