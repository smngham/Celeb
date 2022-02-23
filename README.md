# Popularity Rating
--------

## 1. Overview
	 - https://www.mycelebs.com/의 있는 셀럽(스타)들중 기존 수집된 자료 및 필요시 크롤링을 통하여 각 스타들의 인기도 측정
--------

## 2. Labraries Used
	'''
	Python 3.8.8 with jupyter==1.0.0
	pandas==1.2.4 
	selenium==3.141.0
	webdriver-manager==3.4.2
	bs4==
	tqdm==4.59.0
	PyMySQL==1.0.2
	'''
--------

## 3. Detail 
	- 전체 활동 지수= 매체 활동지수+SNS 활동지수
	- 매체 활동지수
		- 방송수 : 처음부터 유명한 사람이 아닌 이상 방송에 많이 노출되어야 광고를 많이 찍기 때문에 방송수와 광고수의 비중을 높게 잡음 
		- 영화수
		- 공연수
		- 광고수 : 처음부터 유명한 사람이 아닌 이상 방송에 많이 노출되어야 광고를 많이 찍기 때문에 방송수와 광고수의 비중을 높게 잡음
		- 앨범 발매수(Vibe 기준)
	
	- SNS 활동지수
		- 유투브 게시물 수
		- 인스타 게시물 수
		- 트위터 게시물 수
		- VLIVE 영상 수
	
	- 인기도 가중치
		- 유투브 구독자 수(+ 뷰 수, 좋아요 수)
		- 인스타 팔로워 수(+ 좋아요 수, 댓글 수)
		- 트위터 팔로워 수(+좋아요 수, 댓글 수)
		- VLIVE 팔로워(+ 재생 수, 좋아요, 댓글 수)
		- 수상경력 (네이버 인물 프로필 기준)

	- 추가 인기도 가중치
		- 가온 지수(gaon chart)
		- 방송순위
		- 영화순위
		- 관심도
		- 문서량(스타 txt 수집) 
--------

## 4. Code
	- vibe_crawl.py
		-
		
	- youtube_crawl.py
		-

--------
