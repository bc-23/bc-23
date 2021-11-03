

// Current openg date set to January 1 2022

const	countDown = () => {
	const countDate = new Date ("January 1, 2022 00:00:00").getTime();
	const now = new Date().getTime();
	const gap = countDate - now;

const second = 1000;
const minute = second*60;
const hour = minute * 60;
const day = hour * 24; 

const textDay = Math.floor(gap/day);
const texthour = Math.floor((gap%day)/hour);
const textminute = Math.floor((gap%hour)/minute);
const textsecond = Math.floor((gap%minute)/second); 

document.querySelector('.day').innerHTML=textDay;
document.querySelector('.hour').innerHTML=texthour;
document.querySelector('.minute').innerHTML=textminute;
document.querySelector('.second').innerHTML=textsecond;
};

setInterval(countDown, 1000);

function logger(){
console.log('party time');
console.log('party time');
}

logger();

