package worker

import (
	"context"
	"encoding/json"
	"errors"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/social"
	"log"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	redis "github.com/redis/go-redis/v9"
)

const (
	fanoutBatchSize          = 100
	fanoutCelebrityThreshold = 10000
	authorOutboxCap          = 50
	followerInboxCap         = 500
	feedInboxTTL             = 24 * time.Hour
	followerCountTTL         = 5 * time.Minute
)

type FanoutWorker struct {
	ch     *amqp.Channel
	social *social.SocialRepository
	cache  *rediscache.Client
	queue  string
}

func NewFanoutWorker(ch *amqp.Channel, socialRepo *social.SocialRepository, cache *rediscache.Client, queue string) *FanoutWorker {
	return &FanoutWorker{ch: ch, social: socialRepo, cache: cache, queue: queue}
}

func (w *FanoutWorker) Run(ctx context.Context) error {
	if w == nil || w.ch == nil || w.social == nil || w.cache == nil {
		return errors.New("fanout worker is not initialized")
	}
	if w.queue == "" {
		return errors.New("queue is required")
	}

	deliveries, err := w.ch.Consume(
		w.queue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return errors.New("deliveries channel closed")
			}
			w.handleDelivery(ctx, d)
		}
	}
}

func (w *FanoutWorker) handleDelivery(ctx context.Context, d amqp.Delivery) {
	const maxRetries = 3
	for i := 0; i <= maxRetries; i++ {
		select {
		case <-ctx.Done():
			_ = d.Nack(false, true)
			return
		default:
		}
		if err := w.process(ctx, d.Body); err != nil {
			if i >= maxRetries {
				log.Printf("fanout worker: failed after %d retries, dropping: %v", maxRetries, err)
				_ = d.Ack(false)
				return
			}
			wait := time.Duration(1<<uint(i)) * time.Second
			log.Printf("fanout worker: process failed, retrying in %v (%d/%d): %v", wait, i+1, maxRetries, err)
			time.Sleep(wait)
			continue
		}
		_ = d.Ack(false)
		return
	}
}

func (w *FanoutWorker) process(ctx context.Context, body []byte) error {
	var evt rabbitmq.FanoutEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		return nil
	}
	if evt.VideoID == 0 || evt.AuthorID == 0 || evt.CreateTime == 0 {
		return nil
	}

	member := strconv.FormatUint(uint64(evt.VideoID), 10)
	score := float64(evt.CreateTime)
	opCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	authorKey := w.cache.Key("user_videos:%d", evt.AuthorID)
	if err := w.cache.ZAdd(opCtx, authorKey, redis.Z{Score: score, Member: member}); err != nil {
		return err
	}
	_ = w.cache.ZRemRangeByRank(opCtx, authorKey, 0, -authorOutboxCap-1)
	_ = w.cache.Expire(opCtx, authorKey, feedInboxTTL)

	count, err := w.followerCount(ctx, evt.AuthorID)
	if err != nil {
		return err
	}
	if count >= fanoutCelebrityThreshold {
		return nil
	}

	var afterID uint
	for {
		ids, err := w.social.ListFollowerIDs(ctx, evt.AuthorID, afterID, fanoutBatchSize)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}

		items := make(map[string]redis.Z, len(ids))
		for _, id := range ids {
			items[w.cache.Key("feed:inbox:%d", id)] = redis.Z{Score: score, Member: member}
			afterID = id
		}

		batchCtx, batchCancel := context.WithTimeout(ctx, time.Second)
		err = w.cache.ZAddExpireTrimBatch(batchCtx, items, feedInboxTTL, followerInboxCap)
		batchCancel()
		if err != nil {
			return err
		}
		if len(ids) < fanoutBatchSize {
			return nil
		}
	}
}

func (w *FanoutWorker) followerCount(ctx context.Context, authorID uint) (int64, error) {
	key := w.cache.Key("social:follower_count:%d", authorID)
	cacheCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	b, err := w.cache.GetBytes(cacheCtx, key)
	cancel()
	if err == nil {
		if n, parseErr := strconv.ParseInt(string(b), 10, 64); parseErr == nil {
			return n, nil
		}
	}

	count, err := w.social.CountFollowers(ctx, authorID)
	if err != nil {
		return 0, err
	}
	setCtx, setCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	_ = w.cache.SetBytes(setCtx, key, []byte(strconv.FormatInt(count, 10)), followerCountTTL)
	setCancel()
	return count, nil
}
