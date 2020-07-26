// sqlDays.cpp : Este archivo contiene la función "main". La ejecución del programa comienza y termina ahí.
//

#include "pch.h"
#include <iostream>
#include "jdbc/mysql_connection.h"
#include "jdbc/cppconn/driver.h"
#include "jdbc/cppconn/exception.h"
#include "jdbc/cppconn/resultset.h"
#include "jdbc/cppconn/statement.h"
#include "jdbc/cppconn/prepared_statement.h"

int main()
{
	sql::Driver *driver;
	sql::Connection *con;
	sql::Statement *stmt;
	sql::ResultSet  *res;
    std::cout << "Hello World!\n"; 

	driver = get_driver_instance();
	con = driver->connect("tcp://127.0.0.1:3306", "root", "root");
	std::cout << con->isValid() << std::endl;

	stmt = con->createStatement();
	stmt->execute("USE WORLD");
	res = stmt->executeQuery("SELECT * FROM country");
	while (res->next()) {
		// You can use either numeric offsets...
		std::cout << "id = " << res->getString(1); // getInt(1) returns the first column
		// ... or column names for accessing results.
		// The latter is recommended.
		//std::cout << ", label = '" << res->getString("label") << "'" << std::endl;
	}

	delete res;
	delete stmt;
	delete con;
}

// Ejecutar programa: Ctrl + F5 o menú Depurar > Iniciar sin depurar
// Depurar programa: F5 o menú Depurar > Iniciar depuración

// Sugerencias para primeros pasos: 1. Use la ventana del Explorador de soluciones para agregar y administrar archivos
//   2. Use la ventana de Team Explorer para conectar con el control de código fuente
//   3. Use la ventana de salida para ver la salida de compilación y otros mensajes
//   4. Use la ventana Lista de errores para ver los errores
//   5. Vaya a Proyecto > Agregar nuevo elemento para crear nuevos archivos de código, o a Proyecto > Agregar elemento existente para agregar archivos de código existentes al proyecto
//   6. En el futuro, para volver a abrir este proyecto, vaya a Archivo > Abrir > Proyecto y seleccione el archivo .sln
