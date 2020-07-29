// sqlDays.cpp : Este archivo contiene la función "main". La ejecución del programa comienza y termina ahí.
//

#include "pch.h"
#include "csv.h"
#include <iostream>
#include <thread>
#include <mutex>
#include <vector>
#include <sstream>
#include <time.h>
#include <fstream>
#include "jdbc/mysql_connection.h"
#include "jdbc/cppconn/driver.h"
#include "jdbc/cppconn/exception.h"
#include "jdbc/cppconn/resultset.h"
#include "jdbc/cppconn/statement.h"
#include "jdbc/cppconn/prepared_statement.h"

std::mutex mu;
using namespace csv;

static const std::string APP_VERSION = "1.1.0";

std::ofstream outHFile;
std::ofstream outLFile;

class dayClass
{
public:
	std::string day;
	std::vector<std::string> hour;
	std::vector<float> open;
	std::vector<float> high;
	std::vector<float> low;
	std::vector<float> close;

	dayClass(std::string d)
		:day(d)
	{}

	void dayPushback(std::string a, float o, float h, float l, float c)
	{
		hour.push_back(a);
		open.push_back(o);
		high.push_back(h);
		low.push_back(l);
		close.push_back(c);
		return;
	};

};

void showHelpMsg()
{
	std::cout << "version " << APP_VERSION << std::endl << std::endl;
	std::cout << "Usage: daycomp.exe -i file1.csv file2.csv -o output -t [min/sec] " << std::endl << std::endl;
	std::cout << "\t -i input csv files' names. Files' names must not contain white spaces" << std::endl;
	std::cout << "\t -h help. Show this message " << std::endl << std::endl;

	return;
}

std::vector<std::string> split(const std::string &s, char delim) {
	std::vector<std::string> result;
	std::stringstream ss(s);
	std::string item;

	while (getline(ss, item, delim)) {
		result.push_back(item);
	}

	return result;
}

void process_day(dayClass day, unsigned long int begin, unsigned long int end)
{
	sql::Driver *t_driver;
	t_driver = get_driver_instance();
	sql::Connection *con;
	//sql::PreparedStatement  *prep_stmt_l;
	//sql::PreparedStatement  *prep_stmt_h;
	sql::Statement *stmt;

	float lowF;
	float highF;
	long int counter = 0;
	std::vector<std::string> lowDays;
	std::vector<std::string> highDays;
	std::string pS;
	std::stringstream ssl;
	std::stringstream ssh;

	mu.lock();
	std::cout << "begin " << begin << " end " << end << std::endl;
	mu.unlock();

	try
	{

		con = t_driver->connect("tcp://127.0.0.1:3306", "root", "root");
		stmt = con->createStatement();
		con->setSchema("days");
	}
	catch (sql::SQLException &e) {
		std::cout << "# ERR: " << e.what();
		std::cout << " (MySQL error code: " << e.getErrorCode();
		std::cout << ", SQLState: " << e.getSQLState() << " )" << std::endl;
	}

	for (int i = begin; i < end; i++)
	{
		for (long int j = (i + 1); j < day.low.size(); j++) //day.low.size()
		{
			lowF = day.low[i] - day.low[j];
			highF = day.high[i] - day.high[j];
			if (lowF < 4)
			{
				pS = day.hour[i];
				pS.append("-").append(day.hour[j]);
				lowDays.push_back(pS);
			}
			if (highF > -4)
			{
				pS = day.hour[i];
				pS.append("-").append(day.hour[j]);
				highDays.push_back(pS);
			}
			/*
			mu.lock();
			std::cout << begin << " " << day.hour[i] << "-" << day.hour[j] << std::endl;
			mu.unlock();
			*/
		}

		

		try
		{
			//prep_stmt_l = con->prepareStatement("INSERT INTO low(name) VALUES (?) ON DUPLICATE KEY UPDATE count = count + 1;");
			//prep_stmt_h = con->prepareStatement("INSERT INTO high(name) VALUES (?) ON DUPLICATE KEY UPDATE count = count + 1;");

			//stmt->execute("START TRANSACTION;");
			if (lowDays.size() > 0)
			{
				ssl << "INSERT INTO low (name) VALUES ";
				for (long int k = 0; k < lowDays.size(); k++)
				{
					ssl << "('" << lowDays[k];
					if (k < (lowDays.size() - 1))
						ssl << "'), ";
					else
						ssl << "')";
					//prep_stmt_l->setString(1, s);
					//prep_stmt_l->execute();
				}
				//ssl.seekp(-1, std::ios_base::end);
				ssl << " ON DUPLICATE KEY UPDATE count = count + 1;";
				//ssl << ";";
				//mu.lock();
				//std::cout << ssl.str();
				//mu.unlock();
				stmt->execute(ssl.str());
				std::stringstream().swap(ssl);
				//stmt->execute("COMMIT;");
				lowDays.clear();
			}


			if (highDays.size() > 0)
			{
				//stmt->execute("START TRANSACTION;");
				ssh << "INSERT INTO high (name) VALUES ";
				for (long int k = 0; k < highDays.size(); k++)
				{
					ssh << "('" << highDays[k];
					if (k < (highDays.size() - 1))
						ssh << "'),";
					else
						ssh << "')";
					//prep_stmt_l->setString(1, s);
					//prep_stmt_l->execute();
				}
				//ssh.seekp(-1, std::ios_base::end);
				ssh << " ON DUPLICATE KEY UPDATE count = count + 1;";
				stmt->execute(ssh.str());
				std::stringstream().swap(ssh);
				//stmt->execute("COMMIT;");
				highDays.clear();
			}

		}
		catch (sql::SQLException &e) {
			std::cout << "# ERR: " << e.what();
			std::cout << " (MySQL error code: " << e.getErrorCode();
			std::cout << ", SQLState: " << e.getSQLState() << " )" << std::endl;
		}

		if (i % 120 == 0)
			std::cout << day.hour[i] << std::endl;
	}

	delete stmt;
	//delete prep_stmt_l;
	//delete prep_stmt_h;
	delete con;

}

int main(int argc, char **argv)
{
	time_t t = time(NULL);
	struct tm *startT = gmtime(&t);
	int stH = startT->tm_hour;
	int stM = startT->tm_min;
	int stS = startT->tm_sec;
	struct tm *endT;
	std::vector<std::string> in_v;
	std::vector<std::string> inputFileNames;
	std::stringstream ss;

	sql::Driver *driver;
	sql::Connection *con;
	sql::Statement *stmt;
	sql::ResultSet  *res;
	std::cout << "Hello World!\n";

	// Argument parsing
	if (argc > 1)
	{
		for (int argi = 1; argi < argc; argi++)
		{
			if (strcmp(argv[argi], "-i") == 0)
			{
				// input files
				argi++;
				if ((argi < argc) && (argv[argi][0] != '-'))
				{
					while ((argi < argc) && (argv[argi][0] != '-'))
					{
						inputFileNames.push_back(argv[argi]);
						argi++;
					}

					if (argi < argc)
					{
						argi--;
					}
				}
				else
				{
					std::cout << "Error in input file name. Use -h for help" << std::endl;
					return 0;
				}
			}
			else if (strcmp(argv[argi], "-h") == 0)
			{
				showHelpMsg();
				return 0;
			}
		}
	}
	else
	{
		//inputFileName = "./the super boring stuf1.csv";
		showHelpMsg();
		return 0;
	}

	if (inputFileNames.size() == 0)
	{
		std::cout << "There must be entered at least one input file name" << std::endl;
		std::cout << "Use -h for help" << std::endl;
		return 0;
	}

	try
	{
		driver = get_driver_instance();
		sql::ConnectOptionsMap connection_properties;

		connection_properties["hostName"] = "tcp://127.0.0.1";
		connection_properties["userName"] = "root";
		connection_properties["password"] = "root";
		connection_properties["port"] = 3306;
		connection_properties["OPT_RECONNECT"] = true;
		connection_properties["CLIENT_LOCAL_FILES"] = true;

		con = driver->connect("tcp://127.0.0.1:3306", "root", "root");
		//con = driver->connect(connection_properties);
		stmt = con->createStatement();
		stmt->execute("set global max_prepared_stmt_count=200000;");
		stmt->execute("SET GLOBAL local_infile = 'ON';");

		stmt->execute("CREATE SCHEMA IF NOT EXISTS days;");
		con->setSchema("days");
		stmt->execute("DROP TABLE IF EXISTS low");
		stmt->execute("DROP TABLE IF EXISTS high");
		stmt->execute("CREATE TABLE IF NOT EXISTS low(name CHAR(24) PRIMARY KEY, count INTEGER DEFAULT 0) ENGINE=InnoDB;");
		stmt->execute("CREATE TABLE IF NOT EXISTS high(name CHAR(24) PRIMARY KEY, count INTEGER DEFAULT 0) ENGINE=InnoDB;");

		/*ss << "mysql -u root --database=days --password=root -e \"";
		ss << "LOAD DATA LOCAL INFILE ";
		ss << "'07_08low.csv' ";
		ss << "INTO TABLE high ";
		ss << "FIELDS TERMINATED BY ',' ";
		ss << "LINES TERMINATED BY '\\n' ";
		ss << "IGNORE 1 LINES ";
		ss << "(name, count);\"";

		std::cout << ss.str() << std::endl;
		system(ss.str().c_str());*/
		//stmt->execute(ss.str());

		//std::cout << "loaded " << std::endl;

		outHFile.open("oh.csv");
		res = stmt->executeQuery("SELECT name,count FROM low ORDER BY name ASC");

		while (res->next()) {
			// You can use either numeric offsets...
			outHFile << res->getString(1) << "," << res->getInt(2) << "\n";
		}
		
		delete stmt;
		delete con;

	} catch (sql::SQLException &e) {
		std::cout << "# ERR: " << e.what();
		std::cout << " (MySQL error code: " << e.getErrorCode();
		std::cout << ", SQLState: " << e.getSQLState() << " )" << std::endl;
	}

	std::cout << "Start processing at " << startT->tm_hour << ":" << startT->tm_min << ":" << startT->tm_sec << std::endl;

	std::vector<dayClass> oneDay;

	CSVFormat format;
	format.delimiter(',');
	format.variable_columns(false); // Short-hand
	int dayIdx = 0;
	//format.trim({ ' ', '\t' });
	//format.variable_columns(VariableColumnPolicy::IGNORE);
	for (auto& filen : inputFileNames)
	{
		try
		{
			CSVReader reader(filen, format);


			CSVRow row;
			std::string currentDay = "";
			std::vector< std::string> parsStamp;

			for (CSVRow& row : reader)
			{
				if (row[" Open Price"].is_num())
				{
					parsStamp = split(row[" Time"].get<std::string>(), 'T');
					if (parsStamp[0].compare(currentDay) != 0) // nuevo día
					{
						currentDay = parsStamp[0];
						oneDay.push_back(dayClass(parsStamp[0]));
						dayIdx++;
					}
					oneDay[dayIdx - 1].dayPushback(parsStamp[1], atof(row[" Open Price"].get().c_str())
						, atof(row[" High Price"].get().c_str())
						, atof(row[" Low Price"].get().c_str())
						, atof(row[" Close Price"].get().c_str()));
				}
			}
			std::cout << filen << " file readed \n";
		}
		catch (const char* e)
		{
			std::cout << "File error " << filen << e;
			exit(1);
		}
	}

	for (dayClass& d : oneDay)
	{
		std::cout << d.day << " with " << d.hour.size() << " entries " << std::endl;
	}

	std::vector<std::thread> t_threads;

	for (dayClass& d : oneDay)
	{
		t_threads.push_back(std::thread(process_day, d, 0, d.hour.size() / 4));
		t_threads.push_back(std::thread(process_day, d, d.hour.size() / 4, d.hour.size() / 2));
		t_threads.push_back(std::thread(process_day, d, d.hour.size() / 2, d.hour.size() * 3 / 4));
		t_threads.push_back(std::thread(process_day, d, d.hour.size() * 3 / 4, d.hour.size()));

		for (std::thread& t : t_threads)
		{
			t.join();
		}
	}

	time_t te = time(NULL);
	endT = gmtime(&te);
	std::cout << "Start processing at " << stH << ":" << stM << ":" << stS << std::endl;
	std::cout << "End processing at " << endT->tm_hour << ":" << endT->tm_min << ":" << endT->tm_sec << std::endl;
}
