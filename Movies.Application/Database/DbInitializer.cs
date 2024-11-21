using Dapper;

namespace Movies.Application.Database;

public class DbInitializer
{
    private readonly IDbConnectionFactory _dbConnectionFactory;

    public DbInitializer(IDbConnectionFactory dbConnectionFactory)
    {
        _dbConnectionFactory = dbConnectionFactory;
    }

    public async Task InitializeAsync()
    {
        using var connection = await _dbConnectionFactory.CreateConnectionAsync();

        // Create movies table if it doesn't exist
        await connection.ExecuteAsync(
            """
                create table if not exists movies (
                    id CHAR(36) primary key,
                    slug VARCHAR(255) not null,
                    title VARCHAR(255) not null,
                    yearofrelease INTEGER not null
                );
            """);

        // Check if the index exists, and create it if it does not
        var indexExists = await connection.ExecuteScalarAsync<int>(
            """
                SELECT COUNT(*) FROM information_schema.statistics
                WHERE table_schema = DATABASE() AND table_name = 'movies' AND index_name = 'movies_slug_idx';
            """);

        if (indexExists == 0)
        {
            await connection.ExecuteAsync(
                """
                    create unique index movies_slug_id on movies (slug);
                """);
        }

        // Create generes table if it doesn't exist
        await connection.ExecuteAsync(
            """
                create table if not exists genres (
                    movieId CHAR(36) references movies (Id),
                    name VARCHAR(255) not null
                );
            """);

        // Create ratings table if it doesn't exist
        await connection.ExecuteAsync(
            """
                create table if not exists ratings (
                    userid CHAR(36),
                    movieid CHAR(36) references movies (id),
                    rating integer not null,
                    primary key (userid, movieid)
                );
            """);
    }

}
