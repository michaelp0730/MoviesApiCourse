using Dapper;
using Movies.Application.Database;
using Movies.Application.Models;

namespace Movies.Application.Repositories;

public class MovieRepository : IMovieRepository
{
    private readonly IDbConnectionFactory _dbConnectionFactory;

    public MovieRepository(IDbConnectionFactory dbConnectionFactory)
    {
        _dbConnectionFactory = dbConnectionFactory;
    }

    public async Task<bool> CreateAsync(Movie movie)
    {
        using var connection = await _dbConnectionFactory.CreateConnectionAsync();
        using var transaction = connection.BeginTransaction();

        try
        {
            var result = await connection.ExecuteAsync(new CommandDefinition(
                """
                    insert into movies (id, slug, title, yearofrelease)
                    values (@Id, @Slug, @Title, @YearOfRelease)
                """, movie, transaction: transaction
            ));

            if (result > 0)
            {
                foreach (var genre in movie.Genres)
                {
                    await connection.ExecuteAsync(new CommandDefinition(
                        """
                            insert into genres (movieId, name)
                            values (@MovieId, @Name)
                        """, new { MovieId = movie.Id, Name = genre },
                        transaction: transaction
                    ));
                }
            }

            transaction.Commit(); // Commit only if everything succeeds
            return result > 0;
        }
        catch
        {
            transaction.Rollback(); // Rollback if any error occurs
            throw;
        }
    }

    public async Task<Movie?> GetByIdAsync(Guid id)
    {
        using var connection = await _dbConnectionFactory.CreateConnectionAsync();
        var movie = await connection.QuerySingleOrDefaultAsync<Movie>(new CommandDefinition(
            """
                select * from movies where id = @Id
            """, new { id }
        ));

        if (movie is null) return null;

        var genres = await connection.QueryAsync<string>(new CommandDefinition(
            """
                select name from genres where movieid = @id
            """, new { id }
        ));

        foreach (var genre in genres) movie.Genres.Add(genre);

        return movie;
    }

    public async Task<Movie?> GetBySlugAsync(string slug)
    {
        using var connection = await _dbConnectionFactory.CreateConnectionAsync();
        var movie = await connection.QuerySingleOrDefaultAsync<Movie>(new CommandDefinition(
            """
                select * from movies where slug = @Slug
            """, new { slug }
        ));

        if (movie is null) return null;

        var genres = await connection.QueryAsync<string>(new CommandDefinition(
            """
                select name from genres where movieid = @id
            """, new { id = movie.Id }
        ));

        foreach (var genre in genres) movie.Genres.Add(genre);

        return movie;
    }

    public async Task<IEnumerable<Movie>> GetAllAsync()
    {
        using var connection = await _dbConnectionFactory.CreateConnectionAsync();
        var result = await connection.QueryAsync(new CommandDefinition(
            """
                select m.*, GROUP_CONCAT(g.name SEPARATOR ',') as genres
                from movies m 
                left join genres g on m.id = g.movieid
                group by m.id
            """
        ));

        return result.Select(x => new Movie
        {
            Id = x.id,
            Title = x.title,
            YearOfRelease = x.yearofrelease,
            Genres = ((string)x.genres)?.Split(',').ToList() ?? new List<string>() // Explicitly cast genres to string
        });
    }

    public async Task<bool> UpdateAsync(Movie movie)
    {
        using var connection = await _dbConnectionFactory.CreateConnectionAsync();
        using var transaction = connection.BeginTransaction();

        await connection.ExecuteAsync(new CommandDefinition(
            """
                delete from genres where movieid = @Id 
            """, new { id = movie.Id }, transaction: transaction
        ));

        foreach (var genre in movie.Genres)
        {
            await connection.ExecuteAsync(new CommandDefinition(
                """
                    insert into genres (movieId, name) values (@MovieId, @Name)
                """, new { MovieId = movie.Id, Name = genre },
                transaction: transaction
            ));
        }

        var result = await connection.ExecuteAsync(new CommandDefinition(
            """
                update movies set slug = @Slug, title = @Title, yearofrelease = @YearOfRelease
                where id = @Id
            """, movie, transaction: transaction
        ));

        transaction.Commit();
        return result > 0;
    }

    public async Task<bool> DeleteAsync(Guid id)
    {
        using var connection = await _dbConnectionFactory.CreateConnectionAsync();
        using var transaction = connection.BeginTransaction();

        await connection.ExecuteAsync(new CommandDefinition(
            """
                delete from genres where movieId = @Id
            """, new { id }, transaction: transaction
        ));

        var result = await connection.ExecuteAsync(new CommandDefinition(
            """
                delete from movies where id = @Id
            """, new { id }, transaction: transaction
        ));

        transaction.Commit();
        return result > 0;
    }

    public async Task<bool> ExistsByIdAsync(Guid id)
    {
        using var connection = await _dbConnectionFactory.CreateConnectionAsync();
        return await connection.ExecuteScalarAsync<bool>(new CommandDefinition(
            """
                select count(1) from movies where id = @Id
            """, new { id }
        ));
    }
}
