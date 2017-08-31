@echo off

cd ./src
if %errorlevel% neq 0 exit /b %errorlevel%

echo building...
dotnet build --configuration release
if %errorlevel% neq 0 exit /b %errorlevel%

echo running tests. make sure cosmosdb emulator is running...
dotnet test --no-build ./Tests/Tests.csproj
if %errorlevel% neq 0 exit /b %errorlevel%

echo packaging...
dotnet pack ./Client/Client.csproj --no-build --configuration release --output nupkgs
if %errorlevel% neq 0 exit /b %errorlevel%

echo publishing...
cd ./Client/nupkgs
start .
start https://www.nuget.org/packages/manage/upload
cd ../../../