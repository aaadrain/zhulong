# coding:utf-8

import pandas as pd
import tensorflow as tf
import glob
import numpy as np


class SecurityCode(object):
    def __init__(self):
        pass

    def read_csv(self):
        """
        解析CSV文件, 建立文件名和标签值对应表格
        :return:
        """
        csv_data = pd.read_csv("./GenPics/labels.csv", names=["file_num", "chars"], index_col="file_num")
        # print(csv_data)

        num_list = []
        for label in csv_data["chars"]:
            tmp = []
            for letter in label:
                # print(letter)
                tmp.append(ord(letter) - ord("A"))
            num_list.append(tmp)
        # print(num_list)

        csv_data["labels"] = num_list
        # print(csv_data)

        return csv_data

    def read_pic(self):
        # print(glob.glob("data/GenPics/*.jpg"))
        file_list = glob.glob("GenPics/*.jpg")
        # 构造文件队列
        file_queue = tf.train.string_input_producer(file_list)
        # 读取和解码
        reader = tf.WholeFileReader()
        key, value = reader.read(file_queue)
        image_decoded = tf.image.decode_jpeg(value)
        # 批处理
        # print(image_decoded)
        image_decoded.set_shape([20, 80, 3])
        filename_batch, image_batch = tf.train.batch([key, image_decoded], batch_size=100, num_threads=2, capacity=32)
        # print(filename_batch, image_batch)

        return filename_batch, image_batch

    def filename2label(self, filenames, csv_data):
        labels = []
        for filename in filenames:
            # print(filename)
            # print("".join(list(filter(str.isdigit, str(filename)))))
            file_num = "".join(list(filter(str.isdigit, str(filename))))
            labels.append(csv_data.loc[int(file_num), "labels"])
        # print(np.array(labels))
        return np.array(labels)


    # 定义一个生成变量的模式
    def creat_v(self, shape):
        return tf.Variable(tf.random_normal(shape=shape, mean=0, stddev=0.01))

    def cnn_security_code(self):
        """
        完成卷积神经网络
        :param x:
        :return:
        """
        # 定义x,y
        x = tf.placeholder(dtype=tf.float32, shape=[None, 20, 80, 3])
        y = tf.placeholder(dtype=tf.float32, shape=[None, 26 * 4])

        # 第一大层
        # 卷积层
        filter1 = self.creat_v([5, 5, 3, 32])
        bias1 = self.creat_v([32])
        con_ret1 = tf.nn.conv2d(x, filter1, strides=[1, 1, 1, 1], padding="SAME") + bias1
        print(con_ret1)
        # [None, 20,80,3] --> [None, 20, 80, 32]

        # 激活层
        relu_ret1 = tf.nn.relu(con_ret1)
        print(relu_ret1)

        # 池化层
        pool_ret1 = tf.nn.max_pool(relu_ret1, ksize=[1, 2, 2, 1], strides=[1, 2, 2, 1], padding="SAME")
        print(pool_ret1)
        # [None, 20, 80, 32] --> [None , 10, 40, 32]

        # 第二大层
        # 卷积层
        filter2 = self.creat_v([5, 5, 32, 64])
        bias2 = self.creat_v([64])
        con_ret2 = tf.nn.conv2d(pool_ret1, filter2, strides=[1, 1, 1, 1], padding="SAME") + bias2
        print(con_ret2)
        # [None, 10, 40, 32] --> [None , 10, 40, 64]

        # 激活层
        relu_ret2 = tf.nn.relu(con_ret2)
        print(relu_ret2)

        # 池化层
        pool_ret2 = tf.nn.max_pool(relu_ret2, ksize=[1, 2, 2, 1], strides=[1, 2, 2, 1], padding="SAME")
        print(pool_ret2)
        # [None, 10, 40, 64] --> [None , 5, 20, 64]

        # 全连接层
        # X(None, 5 * 20 * 64) * w(5 * 20 * 64, 26*4) + b([26*4]) = y(None, 26*4)
        x_full = tf.reshape(pool_ret2, [-1, 5 * 20 * 64])
        w_full = self.creat_v([5 * 20 * 64, 26 * 4])
        b_full = self.creat_v([26 * 4])

        y_predict = tf.matmul(x_full, w_full) + b_full
        print(y_predict)

        return x, y, y_predict


    def run(self):
        """
        实现主要逻辑
        :return: None
        """
        # 1.解析CSV文件, 建立文件名和标签值对应表格
        csv_data = self.read_csv()

        # 2.读取图片数据
        filename_batch, image_batch = self.read_pic()

        # 4.建立卷积神经网络模型
        x, y, y_predict = self.cnn_security_code()

        # 5.sigmoid交叉熵损失计算
        loss = tf.reduce_mean(tf.nn.sigmoid_cross_entropy_with_logits(labels=y, logits=y_predict))

        # 6. adam优化
        optimizer = tf.train.AdamOptimizer(0.001).minimize(loss)

        # 7. 计算准确率
        equal_list = tf.reduce_all(
            tf.equal(
                tf.argmax(tf.reshape(y, [-1, 4, 26]), axis=-1),
                tf.argmax(tf.reshape(y_predict, [-1, 4, 26]), axis=-1)), axis=1)
        # print(equal_list)
        accuracy = tf.reduce_mean(tf.cast(equal_list, tf.float32))
        # print(accuracy)

        init = tf.global_variables_initializer()

        # 开启会话,查看filename_batch,image_batch内容
        with tf.Session() as sess:
            sess.run(init)
            coord = tf.train.Coordinator()
            threads = tf.train.start_queue_runners(sess=sess, coord=coord)

            for i in range(1000):
                filenames, images = sess.run([filename_batch, image_batch])
                # print(filenames)
                # print(images)
                labels = self.filename2label(filenames, csv_data)
                # print(labels)
                # print(tf.one_hot(labels, 26).eval())
                # print(tf.reshape(tf.one_hot(labels, 26).eval(), [-1, 4*26]))
                labels_onehot = tf.reshape(tf.one_hot(labels, 26), [-1, 4 * 26]).eval()
                # todo 出现问题原因: labels_onehot应该获取到的是具体值,但是当时没有添加eval()值

                optimizer_sess, loss_sess, accuracy_sess = sess.run([optimizer, loss, accuracy],
                                                                    feed_dict={x: images, y: labels_onehot})

                print("第%d次训练,损失是:%f, 准确率是%f" % (i + 1, loss_sess, accuracy_sess))


            coord.request_stop()
            coord.join(threads)



if __name__ == '__main__':
    security_code = SecurityCode()
    security_code.run()
