using Movies.Api.Mapping;
using Movies.Application;
using Movies.Application.Database;
using MySqlConnector;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddDatabase(GetConnectionString());
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddApplication();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();
app.UseAuthorization();
app.UseMiddleware<ValidationMappingMiddleware>();
app.MapControllers();

var dbInitializer = app.Services.GetRequiredService<DbInitializer>();
await dbInitializer.InitializeAsync();

app.Run();

string GetConnectionString()
{
    var mySqlPassword = Environment.GetEnvironmentVariable("InsuranceApp_MySql_Password");
    var mySqlConnectionStringBuilder = new MySqlConnectionStringBuilder
    {
        Server = "localhost",
        UserID = "root",
        Password = mySqlPassword,
        Database = "InsuranceApp", // using existing local DB from another project
    };

    return mySqlConnectionStringBuilder.ConnectionString;
}
