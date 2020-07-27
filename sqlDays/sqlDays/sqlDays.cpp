// sqlDays.cpp : Este archivo contiene la función "main". La ejecución del programa comienza y termina ahí.
//

#include "pch.h"
#include <iostream>
#include <thread>
#include <mutex>
#include <vector>
#include <sstream>
#include <time.h>
#include "jdbc/mysql_connection.h"
#include "jdbc/cppconn/driver.h"
#include "jdbc/cppconn/exception.h"
#include "jdbc/cppconn/resultset.h"
#include "jdbc/cppconn/statement.h"
#include "jdbc/cppconn/prepared_statement.h"

std::mutex mu;

void process_db(sql::Driver *driver, std::vector<std::string> v_in, long int begin, long int end)
{
	sql::Driver *t_driver;

	mu.lock();
	std::cout << "begin " << begin << "end " << end << std::endl;
	mu.unlock();

	try
	{
		t_driver = get_driver_instance();
		sql::Connection *con;
		sql::PreparedStatement  *prep_stmt;
		sql::Statement *stmt;
		con = t_driver->connect("tcp://127.0.0.1:3306", "root", "root");
		stmt = con->createStatement();
		con->setSchema("days");

		prep_stmt = con->prepareStatement("INSERT INTO date(name) VALUES (?) ON DUPLICATE KEY UPDATE count = count + 1;");

		stmt->execute("START TRANSACTION;");
		for (long int i = begin; i < end; i++)
		{
			prep_stmt->setString(1, v_in[i]);
			prep_stmt->execute();
		}
		stmt->execute("COMMIT;");


		delete stmt;
		delete prep_stmt;
		delete con;

	}catch (sql::SQLException &e) {
		std::cout << "# ERR: " << e.what();
		std::cout << " (MySQL error code: " << e.getErrorCode();
		std::cout << ", SQLState: " << e.getSQLState() << " )" << std::endl;
	}

}

int main()
{
	time_t t = time(NULL);
	struct tm *startT = gmtime(&t);
	int stH = startT->tm_hour;
	int stM = startT->tm_min;
	int stS = startT->tm_sec;
	struct tm *endT;
	std::vector<std::string> in_v;

	sql::Driver *driver;
	sql::Connection *con;
	sql::Statement *stmt;
	sql::ResultSet  *res;
    std::cout << "Hello World!\n"; 

	driver = get_driver_instance();
	con = driver->connect("tcp://127.0.0.1:3306", "root", "root");
	//std::cout << con->isValid() << std::endl;

	//con->setSchema("WORLD");
	stmt = con->createStatement();
	
	
	//stmt->execute("USE WORLD");
	//res = stmt->executeQuery("SELECT * FROM country");
	//try
	//{
		//stmt->execute("CREATE SCHEMA IF NOT EXISTS days;");
		con->setSchema("days");
		//stmt->execute("DROP TABLE IF EXISTS date");
		stmt->execute("CREATE TABLE IF NOT EXISTS date(name CHAR(16) PRIMARY KEY, count INT DEFAULT 1) ENGINE=InnoDB;");
		//sql::PreparedStatement  *prep_stmt = con->prepareStatement("INSERT INTO date(name) VALUES (?) ON DUPLICATE KEY UPDATE count = count + 1;");
		
		
		//stmt->execute("BEGIN;");
		for (long int i = 0; i < 1000000; i++)
		{
			std::stringstream name;
			name << "date " << i;
			in_v.push_back(name.str());

			//prep_stmt->setString(1, name.str());
			//prep_stmt->execute();
		}

		std::vector<std::thread> t_threads;

		t_threads.push_back(std::thread(process_db, driver, in_v, 0, in_v.size() / 4));
		t_threads.push_back(std::thread(process_db, driver, in_v, in_v.size() / 4, in_v.size()/2));
		t_threads.push_back(std::thread(process_db, driver, in_v, in_v.size() / 2, in_v.size() * 3 /4));
		t_threads.push_back(std::thread(process_db, driver, in_v, in_v.size() * 3 / 4, in_v.size()));

		for (std::thread& t : t_threads)
		{
			t.join();
		}


		//stmt->execute("COMMIT;");
		//res = stmt->executeQuery("SHOW COLUMNS FROM country;");
		//while (res->next()) {
			// You can use either numeric offsets...
			//std::cout << "id = " << res->getString(1); // getInt(1) returns the first column
			// ... or column names for accessing results.
			// The latter is recommended.
			//std::cout << ", label = '" << res->getString("label") << "'" << std::endl;

		//}
		//delete res;
		//delete prep_stmt;
	//}
	/*catch (sql::SQLException &e) {
		std::cout << "# ERR: " << e.what();
		std::cout << " (MySQL error code: " << e.getErrorCode();
		std::cout << ", SQLState: " << e.getSQLState() << " )" << std::endl;
	}*/

	std::cout << "table updated" << std::endl;


	//delete stmt;
	//delete con;
	delete stmt;
	delete con;

	time_t te = time(NULL);
	endT = gmtime(&te);
	std::cout << "Start processing at " << stH << ":" << stM << ":" << stS << std::endl;
	std::cout << "End processing at " << endT->tm_hour << ":" << endT->tm_min << ":" << endT->tm_sec << std::endl;
}

// Ejecutar programa: Ctrl + F5 o menú Depurar > Iniciar sin depurar
// Depurar programa: F5 o menú Depurar > Iniciar depuración

// Sugerencias para primeros pasos: 1. Use la ventana del Explorador de soluciones para agregar y administrar archivos
//   2. Use la ventana de Team Explorer para conectar con el control de código fuente
//   3. Use la ventana de salida para ver la salida de compilación y otros mensajes
//   4. Use la ventana Lista de errores para ver los errores
//   5. Vaya a Proyecto > Agregar nuevo elemento para crear nuevos archivos de código, o a Proyecto > Agregar elemento existente para agregar archivos de código existentes al proyecto
//   6. En el futuro, para volver a abrir este proyecto, vaya a Archivo > Abrir > Proyecto y seleccione el archivo .sln
