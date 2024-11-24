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

    public async Task<bool> CreateAsync(Movie movie, CancellationToken cancellationToken = default)
    {
        using var connection = await _dbConnectionFactory.CreateConnectionAsync();
        using var transaction = connection.BeginTransaction();

        try
        {
            var result = await connection.ExecuteAsync(new CommandDefinition(
                """
                    insert into movies (id, slug, title, yearofrelease)
                    values (@Id, @Slug, @Title, @YearOfRelease)
                """, movie, transaction: transaction, cancellationToken: cancellationToken
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

    public async Task<Movie?> GetByIdAsync(Guid id, Guid? userId, CancellationToken cancellationToken = default)
    {
        using var connection = await _dbConnectionFactory.CreateConnectionAsync();
        var movie = await connection.QuerySingleOrDefaultAsync<Movie>(new CommandDefinition(
            """
                select m.*, round(avg(r.rating), 1) as rating, myr.rating as userrating
                from movies m
                left join ratings r on m.id = r.movieid
                left join rating myr on m.id = myr.movieid
                where id = @Id
                group by id, userrating
            """, new { id, userId }, cancellationToken: cancellationToken
        ));

        if (movie is null) return null;

        var genres = await connection.QueryAsync<string>(new CommandDefinition(
            """
                select name from genres where movieid = @id
            """, new { id }, cancellationToken: cancellationToken
        ));

        foreach (var genre in genres) movie.Genres.Add(genre);

        return movie;
    }

    public async Task<Movie?> GetBySlugAsync(string slug, Guid? userId, CancellationToken cancellationToken = default)
    {
        using var connection = await _dbConnectionFactory.CreateConnectionAsync();
        var movie = await connection.QuerySingleOrDefaultAsync<Movie>(new CommandDefinition(
            """
                select m.*, round(avg(r.rating), 1) as rating, myr.rating as userrating
                from movies m
                left join ratings r on m.id = r.movieid
                left join rating myr on m.id = myr.movieid
                where slug = @Slug
                group by id, userrating
            """, new { slug, userId }, cancellationToken: cancellationToken
        ));

        if (movie is null) return null;

        var genres = await connection.QueryAsync<string>(new CommandDefinition(
            """
                select name from genres where movieid = @id
            """, new { id = movie.Id }, cancellationToken: cancellationToken
        ));

        foreach (var genre in genres) movie.Genres.Add(genre);

        return movie;
    }

    public async Task<IEnumerable<Movie>> GetAllAsync(GetAllMoviesOptions options, CancellationToken cancellationToken = default)
    {
        using var connection = await _dbConnectionFactory.CreateConnectionAsync();

        // Dynamically build the ORDER BY clause
        var orderClause = string.Empty;
        if (!string.IsNullOrEmpty(options.SortField))
        {
            orderClause = $@"
            ORDER BY m.{options.SortField} {(options.SortOrder == SortOrder.Ascending ? "ASC" : "DESC")}";
        }

        // Build the full query string
        var query = $@"
        SELECT m.*,
            GROUP_CONCAT(g.name SEPARATOR ',') AS genres,
            ROUND(AVG(r.rating), 1) AS rating,
            myr.rating AS userrating
        FROM movies m
        LEFT JOIN genres g ON m.id = g.movieid
        LEFT JOIN ratings r ON m.id = r.movieid
        LEFT JOIN ratings myr ON m.id = myr.movieid AND myr.userid = @userId
        WHERE (@title IS NULL OR m.title LIKE CONCAT('%', @title, '%'))
        AND (@yearofrelease IS NULL OR m.yearofrelease = @yearofrelease)
        GROUP BY m.id, userrating
        {orderClause}"; // Append the ORDER BY clause

        var result = await connection.QueryAsync(new CommandDefinition(
            query, // Pass the dynamically built query
            new
            {
                userId = options.UserId,
                title = options.Title,
                yearofrelease = options.YearOfRelease
            },
            cancellationToken: cancellationToken
        ));

        return result.Select(x => new Movie
        {
            Id = x.id,
            Title = x.title,
            YearOfRelease = x.yearofrelease,
            Rating = (float?)x.rating,
            UserRating = (int?)x.userrating,
            Genres = ((string)x.genres)?.Split(',').ToList() ?? new List<string>()
        });
    }

    public async Task<bool> UpdateAsync(Movie movie, CancellationToken cancellationToken = default)
    {
        using var connection = await _dbConnectionFactory.CreateConnectionAsync();
        using var transaction = connection.BeginTransaction();

        await connection.ExecuteAsync(new CommandDefinition(
            """
                delete from genres where movieid = @Id
            """, new { id = movie.Id }, transaction: transaction, cancellationToken: cancellationToken
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
            """, movie, transaction: transaction, cancellationToken: cancellationToken
        ));

        transaction.Commit();
        return result > 0;
    }

    public async Task<bool> DeleteAsync(Guid id, CancellationToken cancellationToken = default)
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
            """, new { id }, transaction: transaction, cancellationToken: cancellationToken
        ));

        transaction.Commit();
        return result > 0;
    }

    public async Task<bool> ExistsByIdAsync(Guid id, CancellationToken cancellationToken = default)
    {
        using var connection = await _dbConnectionFactory.CreateConnectionAsync();
        return await connection.ExecuteScalarAsync<bool>(new CommandDefinition(
            """
                select count(1) from movies where id = @Id
            """, new { id }, cancellationToken: cancellationToken
        ));
    }
}
