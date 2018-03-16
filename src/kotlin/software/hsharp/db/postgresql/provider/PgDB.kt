package software.hsharp.db.postgresql.provider

import com.mchange.v2.c3p0.ComboPooledDataSource
import org.osgi.service.component.annotations.Component
import software.hsharp.api.icommon.ICConnection
import software.hsharp.api.icommon.IDatabase
import software.hsharp.api.icommon.IDatabaseSetup
import java.sql.Connection
import java.sql.Driver
import java.sql.DriverManager.registerDriver
import java.sql.DriverManager.setLoginTimeout
import java.util.*

@Component
open class PgDB : IDatabase
{
    private val DRIVER : String = "org.postgresql.Driver"
    private val DEFAULT_CONN_TEST_SQL : String = "SELECT 1"

    /** Connection Timeout in seconds   */
    private val CONNECTION_TIMEOUT = 10;

    private var dataSourceObj : ComboPooledDataSource? = null

    protected val dataSource: ComboPooledDataSource
        get() {
            if ( dataSourceObj == null ) dataSourceObj = ComboPooledDataSource()
            return dataSourceObj!!
        }

    /** Driver                  */
    private val driverObj : org.postgresql.Driver = registerIfNeeded( org.postgresql.Driver() )
    private var driverRegistered : Boolean = false

    private val rand = Random()

    private fun registerIfNeeded( driverInst: org.postgresql.Driver ): org.postgresql.Driver {
        if (!driverRegistered)
        {
            registerDriver (driverInst);
            setLoginTimeout (CONNECTION_TIMEOUT);
            driverRegistered = true;
        }
        return driverInst;
    }

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
            sb.append("# Connections: ").append(dataSource.numConnections)
            sb.append(" , # Busy Connections: ").append(dataSource.numBusyConnections)
            sb.append(" , # Idle Connections: ").append(dataSource.numIdleConnections)
            sb.append(" , # Orphaned Connections: ").append(dataSource.numUnclosedOrphanedConnections)
            sb.append(" , # Min Pool Size: ").append(dataSource.minPoolSize)
            sb.append(" , # Max Pool Size: ").append(dataSource.maxPoolSize)
            sb.append(" , # Max Statements Cache Per Session: ").append(dataSource.maxStatementsPerConnection)
        } catch (e: Exception) {
            sb.append( "EXCEPTION:" + e.toString() )
        }

        return sb.toString()
    }    //	getStatus

    override fun setup(parameters: IDatabaseSetup) {
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

        dataSource.maxStatementsPerConnection = params.maxStatementsPerConnection

        if (params.unreturnedConnectionTimeout > 0) {
            dataSource.unreturnedConnectionTimeout = 1200
            dataSource.isDebugUnreturnedConnectionStackTraces = true
        }

        maxRetries = params.maxRetries
        minWaitSecs = params.minWaitSecs
        maxWaitSecs = params.maxWaitSecs
    }

    override fun connect(connection: ICConnection) {
        dataSource.jdbcUrl = getConnectionURL(connection)
        dataSource.user = connection.dbUid
        dataSource.password = connection.dbPwd
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
        dbName = connection.dbName;
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

    var dbName = ""

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
        }
    }    //	close

    private var maxRetries : Int = 5
    private var minWaitSecs : Int = 2
    private var maxWaitSecs : Int = 10

    override val CachedConnection: Connection
    get() {
        var result : Connection? = null
        var exception : Exception? = null
        var retries = 1

        while ( result == null && retries < maxRetries + 1 ) {
            try {
                result = dataSource.connection
            } catch (ex:Exception) {
                exception = ex
            }
            if ( result == null ) {
                // give it another try (but short, since it can be just wrong username and password)
                val randomNum : Long = (rand.nextInt(maxWaitSecs - minWaitSecs + 1) + minWaitSecs).toLong()
                Thread.sleep(randomNum * 1000)
                try {
                    result = dataSource.connection
                } catch (ex:Exception) {
                    exception = ex
                }
            }

            retries++
        }

        if ( result != null ) {
            // now we know the connection can be acquired (e.g. the correct username and password supplied)
            // let's help the others by trying to close the idle connections if we are still under bigger load
            cleanup(result)
        }
        if ( exception != null ) throw exception
        return result!!
    }

    override fun cleanup(connection: Connection) {
        // try to kill the old idle connections first
        val killCommand = "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE datname = '$dbName' AND pid <> pg_backend_pid() AND state = 'idle' AND state_change < current_timestamp - INTERVAL '1' MINUTE;"
        val stmt = connection.createStatement()
        val rs = stmt.executeQuery( killCommand )
        while (rs.next()) {
            val num = rs.getInt(1)
            println("**** KILLED $num idle transactions")
        }
        rs.close()
    }
}
