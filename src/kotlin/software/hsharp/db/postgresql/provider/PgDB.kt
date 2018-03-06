package software.hsharp.db.postgresql.provider

import com.mchange.v2.c3p0.ComboPooledDataSource
import org.osgi.service.component.annotations.Component
import software.hsharp.api.icommon.ICConnection
import software.hsharp.api.icommon.IDatabase
import software.hsharp.api.icommon.IDatabaseSetup
import java.sql.Driver
import java.sql.DriverManager.registerDriver
import java.sql.DriverManager.setLoginTimeout
import javax.sql.DataSource

@Component
open class PgDB : IDatabase
{
    private val DRIVER : String = "org.postgresql.Driver"
    private val DEFAULT_CONN_TEST_SQL : String = "SELECT 1"

    /** Connection Timeout in seconds   */
    private val CONNECTION_TIMEOUT = 10;

    protected val dataSourceObj : ComboPooledDataSource = ComboPooledDataSource()

    /** Driver                  */
    private val driverObj : org.postgresql.Driver = registerIfNeeded( org.postgresql.Driver() )
    private var driverRegistered : Boolean = false

    private fun registerIfNeeded( driverInst: org.postgresql.Driver ): org.postgresql.Driver {
        if (!driverRegistered)
        {
            registerDriver (driverInst);
            setLoginTimeout (CONNECTION_TIMEOUT);
            driverRegistered = true;
        }
        return driverInst;
    }

    override val dataSource: DataSource
        get() = dataSourceObj
    override val status: String
        get() = doGetStatus()
    override val driver: Driver
        get() = driverObj

    override val defaultSetupParameters : IDatabaseSetup
        get() = PgDatabaseSetup( dataSourceName = "default", checkoutTimeout = 10, unreturnedConnectionTimeout = 10 )

    /**
     * Get Status
     * @return status info
     */
    private fun doGetStatus(): String {
        val sb = StringBuilder()
        try {
            sb.append("# Connections: ").append(dataSourceObj.numConnections)
            sb.append(" , # Busy Connections: ").append(dataSourceObj.numBusyConnections)
            sb.append(" , # Idle Connections: ").append(dataSourceObj.numIdleConnections)
            sb.append(" , # Orphaned Connections: ").append(dataSourceObj.numUnclosedOrphanedConnections)
            sb.append(" , # Min Pool Size: ").append(dataSourceObj.minPoolSize)
            sb.append(" , # Max Pool Size: ").append(dataSourceObj.maxPoolSize)
            sb.append(" , # Max Statements Cache Per Session: ").append(dataSourceObj.maxStatementsPerConnection)
        } catch (e: Exception) {
            sb.append( "EXCEPTION:" + e.toString() )
        }

        return sb.toString()
    }    //	getStatus

    override fun setup(parameters: IDatabaseSetup) {
        val params : PgDatabaseSetup = parameters as PgDatabaseSetup
        dataSourceObj.dataSourceName = params.dataSourceName
        dataSourceObj.driverClass = DRIVER
        dataSourceObj.preferredTestQuery = DEFAULT_CONN_TEST_SQL
        dataSourceObj.idleConnectionTestPeriod = params.idleConnectionTestPeriod
        dataSourceObj.maxIdleTimeExcessConnections = params.maxIdleTimeExcessConnections
        dataSourceObj.maxIdleTime = params.maxIdleTime
        dataSourceObj.isTestConnectionOnCheckin = params.testConnectionOnCheckin
        dataSourceObj.isTestConnectionOnCheckout = params.testConnectionOnCheckout
        dataSourceObj.acquireRetryAttempts = params.acquireRetryAttempts
        if (params.checkoutTimeout > 0)
            dataSourceObj.checkoutTimeout = params.checkoutTimeout

        dataSourceObj.initialPoolSize = params.initialPoolSize
        dataSourceObj.initialPoolSize = params.initialPoolSize
        dataSourceObj.minPoolSize = params.minPoolSize
        dataSourceObj.maxPoolSize = params.maxPoolSize

        dataSourceObj.maxStatementsPerConnection = params.maxStatementsPerConnection

        if (params.unreturnedConnectionTimeout > 0) {
            dataSourceObj.unreturnedConnectionTimeout = 1200
            dataSourceObj.isDebugUnreturnedConnectionStackTraces = true
        }
    }

    override fun connect(connection: ICConnection) {
        dataSourceObj.jdbcUrl = getConnectionURL(connection)
        dataSourceObj.user = connection.dbUid
        dataSourceObj.password = connection.dbPwd
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
                .append(connection.dbHost)
                .append(":").append(connection.dbPort)
                .append("/").append(connection.dbName)
                .append("?encoding=UNICODE");
        if (connection.ssl)
            sb.append( "&ssl=true" )

        return sb.toString();
    }   //  getConnectionString

    /**
     * String Representation
     * @return info
     */
    override fun toString(): String {
        val sb = StringBuilder("DB_PostgreSQL[")
        sb.append(doGetStatus())
        sb.append("]")
        return sb.toString()
    }   //  toString

    fun getNumBusyConnections() : Int {
        return dataSourceObj.numBusyConnections
    }

    fun getJdbcUrl() : String {
        return dataSourceObj.jdbcUrl
    }

    /**
     * Close
     */
    open fun close() {

        try {
            dataSourceObj.close()
        } catch (e: Exception) {
        }
    }    //	close
}