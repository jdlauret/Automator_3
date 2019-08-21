# **Automator 3**
This is tool is used to automate reporting for the Post Install Operations BI Team
#### **What does it do?**
The Automator can execute Python scripts, SQL commands, or retrieve query results and output them in the desired 
format
### <span style="color:red">**WARNING**</span>
You must share everything stored in Google Drive that you want run with JD <jonathan.lauret@vivintsolar.com>. 
Otherwise it will not be accessible by the Automator.
#### **Adding New Tasks / Insert Helper**
There is not currently a built in interface for inserting tasks into the Automator
<br>If you need help with inserting a task into the Task Table.  Use the Insert Helper that is included. It is a excel
sheet with a built in Macro.  It simply generates an insert statement that you can use in your SQL console. It also
has some data validation built in, to help ensure that all the necessary columns have been filled out.
#### **How to run Python scripts**
If you're looking to run a scriptm, it first needs to be sent to JD.  Who will add to the 'script_storage' 
directory. Once added the task will need to be setup in the T_AUTO_TASKS table using the Insert Helper.
#### **How to run SQL Commands**
The SQL command needs to be saved as a <b>'.sql'</b> and then stored in Google Drive
<br>When setting up the task in the Insert Helper you will need to put the Google Drive File ID into the Data Source
column.
#### **How to run SQL Queries**
Running a SQL report is similar to SQL commands.  A **'.sql'** needs to be saved to Google Drive and the File ID set
as the Data Source.  The Insert Helper will help with setting up the data format selections and storage.

##### What Formats are Available?
If a SQL Query was setup, you can select from several format types for the results to be saved in.
  
##### **Current formats**
1. Google Sheets
2. CSV 
3. Excel

