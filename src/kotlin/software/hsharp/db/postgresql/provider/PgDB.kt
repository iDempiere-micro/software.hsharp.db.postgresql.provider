package software.hsharp.db.postgresql.provider

import com.mchange.v2.c3p0.ComboPooledDataSource
import software.hsharp.api.icommon.ICConnection
import software.hsharp.api.icommon.IDatabase
import software.hsharp.api.icommon.IDatabaseSetup
import java.sql.DriverManager
import javax.sql.DataSource

open class PgDB : IDatabase
{
    private val DRIVER : String = "org.postgresql.Driver"
    private val DEFAULT_CONN_TEST_SQL : String = "SELECT 1"

    /** Connection Timeout in seconds   */
    private val CONNECTION_TIMEOUT = 10;


    protected val dataSource : ComboPooledDataSource = ComboPooledDataSource()

	/** Driver                  */
	private val s_driver : org.postgresql.Driver = org.postgresql.Driver()
    private var driverRegistered : Boolean = false

	/**
	 *  Get and register Database Driver
	 *  @return Driver
	 */
	override fun getDriver() : java.sql.Driver
	{
		if (!driverRegistered)
		{
			DriverManager.registerDriver (s_driver);
			DriverManager.setLoginTimeout (CONNECTION_TIMEOUT);
		}
		return s_driver;
	}   //  getDriver

    /**
     * 	Create DataSource (Client)
     *	@param connection connection
     *	@return data dource
     */
    override fun getDataSource() : DataSource
    {
        return dataSource;
    }

    /**
	 *  Get Database Connection String.
	 *  Requirements:
	 *      - createdb -E UNICODE compiere
	 *  @param connection Connection Descriptor
	 *  @return connection String
	 */
    open fun getConnectionURL (connection:ICConnection ) : String
	{
		//  jdbc:postgresql://hostname:portnumber/databasename?encoding=UNICODE
		val sb = StringBuilder("jdbc:postgresql://")
			.append(connection.getDbHost())
			.append(":").append(connection.getDbPort())
			.append("/").append(connection.getDbName())
			.append("?encoding=UNICODE");

		return sb.toString();
	}   //  getConnectionString

    override fun connect(connection: ICConnection) {
        dataSource.jdbcUrl = getConnectionURL(connection)
        dataSource.user = connection.dbUid
        dataSource.password = connection.dbPwd
    }

    override fun setup(parameters: IDatabaseSetup)
    {
        val params : PgDatabaseSetup = parameters as PgDatabaseSetup
        dataSource.dataSourceName = params.dataSourceName
        dataSource.driverClass = DRIVER
        dataSource.preferredTestQuery = DEFAULT_CONN_TEST_SQL
        dataSource.idleConnectionTestPeriod = params.idleConnectionTestPeriod
        dataSource.maxIdleTimeExcessConnections = params.maxIdleTimeExcessConnections
        dataSource.maxIdleTime = params.maxIdleTime
        dataSource.isTestConnectionOnCheckin = params.testConnectionOnCheckin
        dataSource.isTestConnectionOnCheckout = params.testConnectionOnCheckout
        dataSource.acquireRetryAttempts = params.acquireRetryAttempts
        if (params.checkoutTimeout > 0)
            dataSource.checkoutTimeout = params.checkoutTimeout

        dataSource.initialPoolSize = params.initialPoolSize
        dataSource.initialPoolSize = params.initialPoolSize
        dataSource.minPoolSize = params.minPoolSize
        dataSource.maxPoolSize = params.maxPoolSize

        if (params.maxStatementsPerConnection > 0)
            dataSource.maxStatementsPerConnection = params.maxStatementsPerConnection

        if (params.unreturnedConnectionTimeout > 0) {
            dataSource.unreturnedConnectionTimeout = 1200
            dataSource.isDebugUnreturnedConnectionStackTraces = true
        }

    }

    /**
     * String Representation
     * @return info
     */
    override fun toString(): String {
        val sb = StringBuilder("DB_PostgreSQL[")
        try {
            val logBuffer = StringBuilder(50)
            logBuffer.append("# Connections: ").append(dataSource.numConnections)
            logBuffer.append(" , # Busy Connections: ").append(dataSource.numBusyConnections)
            logBuffer.append(" , # Idle Connections: ").append(dataSource.numIdleConnections)
            logBuffer.append(" , # Orphaned Connections: ").append(dataSource.numUnclosedOrphanedConnections)
        } catch (e: Exception) {
            sb.append("=").append(e.localizedMessage)
        }

        sb.append("]")
        return sb.toString()
    }   //  toString

    /**
     * Get Status
     * @return status info
     */
    override fun getStatus(): String? {
        val sb = StringBuilder()
        try {
            sb.append("# Connections: ").append(dataSource.numConnections)
            sb.append(" , # Busy Connections: ").append(dataSource.numBusyConnections)
            sb.append(" , # Idle Connections: ").append(dataSource.numIdleConnections)
            sb.append(" , # Orphaned Connections: ").append(dataSource.numUnclosedOrphanedConnections)
            sb.append(" , # Min Pool Size: ").append(dataSource.minPoolSize)
            sb.append(" , # Max Pool Size: ").append(dataSource.maxPoolSize)
            sb.append(" , # Max Statements Cache Per Session: ").append(dataSource.maxStatementsPerConnection)
        } catch (e: Exception) {
        }

        return sb.toString()
    }    //	getStatus


    fun getNumBusyConnections() : Int {
        return dataSource.numBusyConnections
    }

    fun getJdbcUrl() : String {
        return dataSource.jdbcUrl
    }

    /**
     * Close
     */
    open fun close() {

        try {
            dataSource.close()
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }    //	close

}