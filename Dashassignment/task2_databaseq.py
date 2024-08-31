class Database:
    def __init__(self, columns=None, primary_key=None):
        if columns is None:
            columns = {}
        self.database = []
        self.columnToDatabaseIndexMapping = {}
        self.numColumns = len(columns)
        self.numRows = 0
        self.columnsSchema = columns
        self.sortColumn = primary_key
        index = 0
        for column, colType in columns.items():
            self.columnToDatabaseIndexMapping[column] = index
            index += 1

    @classmethod
    def create(cls, columns, primary_key=None):
        return cls(columns, primary_key)

    def insert(self, row):
        newRow = [None] * self.numColumns
        for column, value in row.items():
            if column in self.columnToDatabaseIndexMapping:
                newRow[self.columnToDatabaseIndexMapping[column]] = value

        self.numRows += 1
        self.database.append(newRow)

    def linearSearch(self, column, value):
        colIndex = self.columnToDatabaseIndexMapping[column]
        for i in range(self.numRows):
            if self.database[i][colIndex] == value:
                return i
        return -1

    def select(self, column, conditionType="=", value=None):
        result = []
        colIndex = self.columnToDatabaseIndexMapping[column]

        if value == "all":
            for row in self.database:
                result.append(row[colIndex])
            return result

        if conditionType == "=":
            idx = self.linearSearch(column, value)
            if idx != -1:
                result.append(self.database[idx])

        elif conditionType == ">":
            for row in self.database:
                if row[colIndex] > value:
                    result.append(row)

        elif conditionType == "<":
            for row in self.database:
                if row[colIndex] < value:
                    result.append(row)

        return result

    def delete(self, column, conditionType="=", value=None):
        colIndex = self.columnToDatabaseIndexMapping[column]
        toDelete = []

        if conditionType == "=":
            for i in range(self.numRows):
                if self.database[i][colIndex] == value:
                    toDelete.append(i)

        elif conditionType == ">":
            for i in range(self.numRows):
                if self.database[i][colIndex] > value:
                    toDelete.append(i)

        elif conditionType == "<":
            for i in range(self.numRows):
                if self.database[i][colIndex] < value:
                    toDelete.append(i)

        for index in sorted(toDelete, reverse=True):
            del self.database[index]
            self.numRows -= 1

    def max(self, column):
        colIndex = self.columnToDatabaseIndexMapping[column]
        if self.numRows == 0:
            return None
        maxValue = self.database[0][colIndex]
        for row in self.database:
            if row[colIndex] > maxValue:
                maxValue = row[colIndex]
        return maxValue

    def sum(self, column):
        colIndex = self.columnToDatabaseIndexMapping[column]
        totalSum = 0
        for row in self.database:
            totalSum += row[colIndex]
        return totalSum

    def join(self, otherDb, columnSelf, columnOther):
        colIndexSelf = self.columnToDatabaseIndexMapping[columnSelf]
        colIndexOther = otherDb.columnToDatabaseIndexMapping[columnOther]

        joinedData = []

        for rowSelf in self.database:
            for rowOther in otherDb.database:
                if rowSelf[colIndexSelf] == rowOther[colIndexOther]:
                    joinedRow = rowSelf + rowOther[:colIndexOther] + rowOther[colIndexOther + 1:]
                    joinedData.append(joinedRow)

        print("Joined Table:")
        for row in joinedData:
            print(row)


if __name__ == "__main__":
    People = Database.create(columns={"id": int, "name": str, "age": int}, primary_key="id")
    Employee = Database.create(columns={"id": int, "department": str, "salary": int}, primary_key="id")

    People.insert({"id": 1, "name": "Mukesh", "age": 30})
    People.insert({"id": 2, "name": "Suresh", "age": 25})
    People.insert({"id": 3, "name": "Ramesh", "age": 35})

    Employee.insert({"id": 1, "department": "Engineering", "salary": 70000})
    Employee.insert({"id": 2, "department": "Marketing", "salary": 600000})
    Employee.insert({"id": 3, "department": "HR", "salary": 80000})

    allnames = People.select("name", "=", "all")
    print("All names:", allnames)

    selected = People.select("name", "=", "Ramesh")
    print("Selected Rows where id = 2:", selected)

    People.delete("age", ">", 30)
    print("Table after deleting rows where age > 30:", People.database)

    People.insert({"id": 3, "name": "Ramesh", "age": 35})

    maxSalary = Employee.max("salary")
    print("Max Salary:", maxSalary)

    sumSalaries = Employee.sum("salary")
    print("Sum of Salaries:", sumSalaries)

    People.join(Employee, "id", "id")
