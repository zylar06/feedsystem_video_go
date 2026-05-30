package worker

import (
	"context"
	"encoding/json"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/video"
	"fmt"
	"log"
	"time"

	oredis "github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

func StartOutboxPoller(db *gorm.DB, tmq *rabbitmq.TimelineMQ, fmq *rabbitmq.FanoutMQ) {
	if db == nil || tmq == nil {
		log.Printf("Outbox poller disabled: timeline mq is not initialized")
		return
	}

	go func() {
		for {
			var messages []video.OutboxMsg

			err := db.Where("status = ?", "pending").Order("create_time ASC").Limit(100).Find(&messages).Error
			if err != nil || len(messages) == 0 {
				time.Sleep(1 * time.Second)
				continue
			}

			for _, msg := range messages {
				if err := tmq.PublishVideo(context.Background(), msg.VideoID, msg.CreateTime); err != nil {
					log.Printf("publish TimelineMQ failed: videoID=%d err=%v", msg.VideoID, err)
					continue
				}

				if fmq != nil {
					var v video.Video
					if err := db.Select("id", "author_id").First(&v, msg.VideoID).Error; err != nil {
						log.Printf("outbox load video failed: videoID=%d err=%v", msg.VideoID, err)
						continue
					}
					if err := fmq.PublishVideo(context.Background(), msg.VideoID, v.AuthorID, msg.CreateTime); err != nil {
						log.Printf("publish FanoutMQ failed: videoID=%d err=%v", msg.VideoID, err)
						continue
					}
				}

				if err := db.Delete(&msg).Error; err != nil {
					log.Printf("delete outbox message failed: id=%d err=%v", msg.ID, err)
				}
			}
		}
	}()
}

func StartConsumer(tmq *rabbitmq.TimelineMQ, queueName string, redisClient *rediscache.Client, rmq *rabbitmq.RabbitMQ) {
	if tmq == nil || rmq == nil || rmq.Conn == nil {
		log.Printf("Timeline consumer disabled: rabbitmq is not initialized")
		return
	}
	if redisClient == nil {
		log.Printf("Timeline consumer disabled: redis is not initialized")
		return
	}

	go func() {
		for {
			ch, err := rmq.NewChannel()
			if err != nil {
				log.Printf("Timeline consumer: create channel failed: %v, retrying in 5s", err)
				time.Sleep(5 * time.Second)
				continue
			}

			if err := ch.Qos(10, 0, false); err != nil {
				log.Printf("Timeline consumer: QoS setup failed: %v", err)
			}

			msgs, err := ch.Consume(queueName, "", false, false, false, false, nil)
			if err != nil {
				log.Printf("Timeline consumer: consume failed: %v, retrying in 5s", err)
				ch.Close()
				time.Sleep(5 * time.Second)
				continue
			}

			log.Printf("Timeline consumer started: queue=%s", queueName)

			for msg := range msgs {
				var event rabbitmq.TimelineEvent
				if err := json.Unmarshal(msg.Body, &event); err != nil {
					log.Printf("Timeline consumer: unmarshal failed: %v", err)
					msg.Ack(false)
					continue
				}

				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				timelineKey := redisClient.Key("feed:global_timeline")
				err = redisClient.ZAdd(ctx, timelineKey, oredis.Z{
					Score:  float64(event.CreateTime),
					Member: fmt.Sprintf("%d", event.VideoID),
				})

				if err != nil {
					log.Printf("Timeline consumer: write zset failed: %v", err)
					msg.Nack(false, true)
					cancel()
					continue
				}

				if err := redisClient.ZRemRangeByRank(ctx, timelineKey, 0, -1001); err != nil {
					log.Printf("Timeline consumer: trim zset failed: %v", err)
				}

				msg.Ack(false)
				cancel()
			}

			ch.Close()
			log.Printf("Timeline consumer: channel closed, reconnecting in 5s")
			time.Sleep(5 * time.Second)
		}
	}()
}
