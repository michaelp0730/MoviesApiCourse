using FluentValidation.Results;
using Movies.Application.Models;
using Movies.Application.Repositories;

namespace Movies.Application.Services;

public class RatingService : IRatingService
{
    private readonly IMovieRepository _movieRepository;
    private readonly IRatingRepository _ratingRepository;

    public RatingService(IMovieRepository movieRepository, IRatingRepository ratingRepository)
    {
        _movieRepository = movieRepository;
        _ratingRepository = ratingRepository;
    }

    public async Task<bool> RateMovieAsync(Guid movieId, int rating, Guid userId, CancellationToken token = default)
    {
        if (rating is <= 0 or > 5)
        {
            throw new FluentValidation.ValidationException(new List<ValidationFailure>
            {
                new ValidationFailure("Rating", "Rating must be between 1 and 5")
            });
        }

        var movieExists = await _movieRepository.ExistsByIdAsync(movieId, token);
        if (!movieExists) return false;

        return await _ratingRepository.RateMovieAsync(movieId, rating, userId, token);
    }

    public Task<bool> DeleteRatingAsync(Guid movieId, Guid userId, CancellationToken token = default)
    {
        return _ratingRepository.DeleteRatingAsync(movieId, userId, token);
    }

    public Task<IEnumerable<MovieRating>> GetRatingsForUserAsync(Guid userId, CancellationToken token = default)
    {
        return _ratingRepository.GetRatingsForUserAsync(userId, token);
    }
}
