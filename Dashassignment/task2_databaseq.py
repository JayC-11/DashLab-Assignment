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
        currentIndex = self.numRows - 1

        while currentIndex > 0 and self.database[currentIndex][self.columnToDatabaseIndexMapping[self.sortColumn]] < self.database[currentIndex - 1][self.columnToDatabaseIndexMapping[self.sortColumn]]:
            self.database[currentIndex], self.database[currentIndex - 1] = self.database[currentIndex - 1], self.database[currentIndex]
            currentIndex -= 1

    def binarySearch(self, column, value):
        low = 0
        high = self.numRows - 1
        colIndex = self.columnToDatabaseIndexMapping[column]

        while low <= high:
            mid = (low + high) // 2
            if self.database[mid][colIndex] == value:
                return mid
            elif self.database[mid][colIndex] < value:
                low = mid + 1
            else:
                high = mid - 1

        return -1

    def select(self, column, conditionType="=", value=None):
        result = []
        colIndex = self.columnToDatabaseIndexMapping[column]

        if value == "all":
            for row in self.database:
                result.append(row[colIndex])
            return result

        if conditionType == "=":
            idx = self.binarySearch(column, value)
            if idx != -1:
                result.append(self.database[idx])

        elif conditionType == ">":
            idx = self.binarySearch(column, value)
            if idx != -1:
                for i in range(idx, self.numRows):
                    if self.database[i][colIndex] > value:
                        result.append(self.database[i])

        elif conditionType == "<":
            idx = self.binarySearch(column, value)
            if idx != -1:
                for i in range(0, idx):
                    if self.database[i][colIndex] < value:
                        result.append(self.database[i])

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

    #Just to see all the values from the a column I've added the "all" functionality
    allnames = People.select("name", "=", "all")
    print("All names:", allnames)

    selected = People.select("id", "=", 2)
    print("Selected Rows where id = 2:", selected)

    People.delete("age", ">", 30)
    print(" after deleting rows where age > 30:", People.database)

    People.insert({"id": 3, "name": "Ramesh", "age": 35})

    max = Employee.max("salary")
    print("Max Salary:", max)

    sum = Employee.sum("salary")
    print("Sum of Salaries:", sum)

    People.join(Employee, "id", "id")


