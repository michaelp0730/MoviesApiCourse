using System.Security.Principal;
using FluentValidation;
using Movies.Application.Models;
using Movies.Application.Repositories;

namespace Movies.Application.Services;

public class MovieService : IMovieService
{
    private readonly IMovieRepository _movieRepository;
    private readonly IValidator<Movie> _movieValidator;
    private readonly IValidator<GetAllMoviesOptions> _optionsValidator;
    private readonly IRatingRepository _ratingRepository;

    public MovieService(
        IMovieRepository movieRepository,
        IValidator<Movie> movieValidator,
        IValidator<GetAllMoviesOptions> optionsValidator,
        IRatingRepository ratingRepository
    )
    {
        _movieRepository = movieRepository;
        _movieValidator = movieValidator;
        _optionsValidator = optionsValidator;
        _ratingRepository = ratingRepository;
    }

    public async Task<bool> CreateAsync(Movie movie, CancellationToken cancellationToken = default)
    {
        await _movieValidator.ValidateAndThrowAsync(movie, cancellationToken: cancellationToken);
        return await _movieRepository.CreateAsync(movie, cancellationToken);
    }

    public Task<Movie?> GetByIdAsync(Guid id, Guid? userId = default, CancellationToken cancellationToken = default)
    {
        return _movieRepository.GetByIdAsync(id, userId, cancellationToken);
    }

    public Task<Movie?> GetBySlugAsync(string slug, Guid? userId = default, CancellationToken cancellationToken = default)
    {
        return _movieRepository.GetBySlugAsync(slug, userId, cancellationToken);
    }

    public async Task<IEnumerable<Movie>> GetAllAsync(GetAllMoviesOptions options, CancellationToken cancellationToken = default)
    {
        await _optionsValidator.ValidateAndThrowAsync(options, cancellationToken);
        return await _movieRepository.GetAllAsync(options, cancellationToken);
    }

    public async Task<Movie?> UpdateAsync(Movie movie, Guid? userId = default, CancellationToken cancellationToken = default)
    {
        await _movieValidator.ValidateAndThrowAsync(movie, cancellationToken: cancellationToken);
        var movieExists = await _movieRepository.ExistsByIdAsync(movie.Id);
        if (!movieExists) return null;

        await _movieRepository.UpdateAsync(movie, cancellationToken);

        if (!userId.HasValue)
        {
            var avgRating = await _ratingRepository.GetRatingAsync(movie.Id, cancellationToken);
            movie.Rating = avgRating;
            return movie;
        }

        var (Rating, UserRating) = await _ratingRepository.GetRatingAsync(movie.Id, userId, cancellationToken);
        movie.Rating = Rating;
        movie.UserRating = UserRating;

        return movie;
    }

    public Task<bool> DeleteAsync(Guid id, CancellationToken cancellationToken = default)
    {
        return _movieRepository.DeleteAsync(id, cancellationToken);
    }
}
