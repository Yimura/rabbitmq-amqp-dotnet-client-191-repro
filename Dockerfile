FROM mcr.microsoft.com/dotnet/sdk:10.0
WORKDIR /src
COPY Repro.csproj .
RUN dotnet restore
COPY Program.cs .
RUN dotnet build -c Release --no-restore
ENTRYPOINT ["dotnet", "run", "-c", "Release", "--no-build"]
